using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using EquityBackend.GameLogic.Service;

namespace EquityBackend.GameLogic;

public struct TradeReceipt
{
    public string Ticker { get; set; }
    public decimal Price { get; set; }
    public decimal Quantity { get; set; }
    public long BuyerId { get; set; }
    public long SellerId { get; set; }
    public DateTime TradeTime { get; set; }
}


// TargetUserId == 0 → 전체 broadcast
public record PushPayload(short Type, long TargetUserId, byte[] Body);

public class AssetTickerInfo
{
    public long CurrentPrice { get; set; }
    public long UpperLimitPrice { get; set; }
    public long LowerLimitPrice { get; set; }
    public long Base { get; set; }
}

public class MatchingEngine
{
    public Dictionary<string, List<AssetRelation>> _relations = new Dictionary<string, List<AssetRelation>>();
    public Dictionary<string, long> _lastPrices = new Dictionary<string, long>();

    private readonly Channel<TradeReceipt> _tradeChannel = Channel.CreateUnbounded<TradeReceipt>();
    private readonly Channel<PushPayload> _pushChannel = Channel.CreateUnbounded<PushPayload>();
    private readonly ConcurrentDictionary<string, OrderBook> _orderBooks = new ConcurrentDictionary<string, OrderBook>();
    private Dictionary<string, AssetTickerInfo> _tickers = new Dictionary<string, AssetTickerInfo>();

    // 종목별 전용 주문 대기열 — 한 종목의 호가창은 항상 전용 워커 1개만 수정한다
    private readonly ConcurrentDictionary<string, Channel<OrderWorkItem>> _tickerQueues =
        new ConcurrentDictionary<string, Channel<OrderWorkItem>>();

    // 대기열 항목 — Completion은 주문이 실제로 처리된 뒤 호출자를 깨우기 위한 것
    private sealed record OrderWorkItem(Order Order, TaskCompletionSource<bool> Completion);

    // 현재가 읽기/쓰기 보호
    private readonly ReaderWriterLockSlim _priceLock = new ReaderWriterLockSlim();

    public ChannelReader<TradeReceipt> TradeReader => _tradeChannel.Reader;
    public ChannelReader<PushPayload> PushReader => _pushChannel.Reader;

    public void InitializeTickers(IEnumerable<Asset> assetFromDb)
    {
        foreach (var asset in assetFromDb)
        {
            AssetTickerInfo tickerInfo = new()
            {
                CurrentPrice = asset.BasePrice,
                UpperLimitPrice = (long)(asset.BasePrice * 1.3),
                LowerLimitPrice = (long)(asset.BasePrice * 0.7),
                Base = asset.BasePrice,
            };
            _tickers[asset.Ticker] = tickerInfo;
            StartTickerWorker(asset.Ticker);
        }
    }

    // 종목당 대기열 1개 + 워커 1개. 중복 호출 시 TryAdd 실패로 워커가 늘어나지 않는다.
    private void StartTickerWorker(string ticker)
    {
        var channel = Channel.CreateUnbounded<OrderWorkItem>(
            new UnboundedChannelOptions { SingleReader = true });

        if (!_tickerQueues.TryAdd(ticker, channel)) return;

        _ = Task.Run(() => RunTickerWorker(ticker, channel.Reader));
    }

    // 들어온 순서대로 하나씩 처리 — 이 메서드만이 해당 종목 호가창의 유일한 수정자
    private async Task RunTickerWorker(string ticker, ChannelReader<OrderWorkItem> reader)
    {
        try
        {
            await foreach (var item in reader.ReadAllAsync())
            {
                try
                {
                    ProcessOrderCore(item.Order);
                    item.Completion.TrySetResult(true);
                }
                catch (Exception ex)
                {
                    // 주문 하나의 실패가 대기열을 멈추지 않도록 호출자에게만 전달
                    item.Completion.TrySetException(ex);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[OrderQueue] {ticker} 워커 종료: {ex.Message}");
        }
    }

    public void InitializeRelations(IEnumerable<AssetRelation> relations)
    {
        _relations.Clear();
        foreach (var rel in relations)
        {
            if (!_relations.ContainsKey(rel.SourceAsset))
                _relations[rel.SourceAsset] = new List<AssetRelation>();
            _relations[rel.SourceAsset].Add(rel);
        }
    }

    // InitializeTickers 호출 직후에 호출해야 함 — 첫 틱 delta=0 방지
    public void InitializeLastPrices()
    {
        foreach (var kvp in _tickers)
            _lastPrices[kvp.Key] = kvp.Value.CurrentPrice;
    }

    // 호가창을 직접 건드리지 않고 종목 전용 대기열에 넣는다.
    // 반환 Task는 그 주문이 실제로 처리된 뒤 완료되므로, 호출자의 예외/응답 코드 처리는 그대로 유지된다.
    public Task ProcessOrder(Order incomingOrder)
    {
        if (!_tickerQueues.TryGetValue(incomingOrder.Ticker, out var queue))
            throw new KeyNotFoundException($"등록되지 않은 종목: {incomingOrder.Ticker}");

        // RunContinuationsAsynchronously — 호출자의 후속 작업(DB 커밋 등)이 워커 스레드를 붙잡지 않도록
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        if (!queue.Writer.TryWrite(new OrderWorkItem(incomingOrder, completion)))
            throw new InvalidOperationException($"주문 대기열 쓰기 실패: {incomingOrder.Ticker}");

        return completion.Task;
    }

    // 종목 전용 워커에서만 호출된다 — 호가창 수정은 전부 이 안에서만 일어난다
    private void ProcessOrderCore(Order incomingOrder)
    {
        AssetTickerInfo assetTickerInfo = _tickers[incomingOrder.Ticker];

        if (assetTickerInfo.LowerLimitPrice > incomingOrder.Price ||
            assetTickerInfo.UpperLimitPrice < incomingOrder.Price)
        {
            throw new Exception("Asset ticker is out of range");
        }

        var orderBook = _orderBooks.GetOrAdd(incomingOrder.Ticker, t => new OrderBook(t));

        // 워커끼리는 대기열로 직렬화되지만, ValueChain 틱의 호가창 읽기와는 이 락으로 분리한다
        lock (orderBook.SyncRoot)
        {
            MatchOrder(incomingOrder, orderBook);

            // 시스템 주문(UserId=0)은 호가창에 남기지 않음 — 잔류 시 실제 주문과 유령 채결 발생
            if (incomingOrder.Quantity > incomingOrder.ExecutedQuantity && incomingOrder.UserId != 0)
            {
                orderBook.AddOrder(incomingOrder);
                Console.WriteLine($"[호가 등록] {incomingOrder.Ticker} {incomingOrder.Side} 대기 등록 - 잔여수량: {incomingOrder.Quantity - incomingOrder.ExecutedQuantity}개");
            }
        }

        // AddOrder 뒤에 스냅샷 — 잔여 주문이 반영된 호가창을 브로드캐스트
        _pushChannel.Writer.TryWrite(BuildOrderBookPush(incomingOrder.Ticker, orderBook));
    }

    // 호출자가 orderBook.SyncRoot를 이미 잡고 있어야 한다
    private void MatchOrder(Order incomingOrder, OrderBook orderBook)
    {
        var oppsiteBook = incomingOrder.Side == OrderSide.Buy ? orderBook.Asks : orderBook.Bids;

        while (incomingOrder.ExecutedQuantity < incomingOrder.Quantity && oppsiteBook.Any())
        {
            //가장 조건이 좋은 가격대
            var bestPriceLevel = oppsiteBook.First();
            long matchPrice = bestPriceLevel.Key;
            var orderQueue = bestPriceLevel.Value;

            // 가격 조건 확인
            if (incomingOrder.Side == OrderSide.Buy && matchPrice > incomingOrder.Price) break;
            if (incomingOrder.Side == OrderSide.Sell && matchPrice < incomingOrder.Price) break;

            while (incomingOrder.ExecutedQuantity < incomingOrder.Quantity && orderQueue.Count > 0)
            {
                var makerOrder = orderQueue.Peek();

                int incomingRemaining = incomingOrder.Quantity - incomingOrder.ExecutedQuantity;
                int makerRemaining = makerOrder.Quantity - makerOrder.ExecutedQuantity;
                int tradeQty = Math.Min(incomingRemaining, makerRemaining);

                incomingOrder.ExecutedQuantity += tradeQty;
                makerOrder.ExecutedQuantity += tradeQty;

                _priceLock.EnterWriteLock();
                try { _tickers[incomingOrder.Ticker].CurrentPrice = matchPrice; }
                finally { _priceLock.ExitWriteLock(); }

                Console.WriteLine($"채결 완료: {incomingOrder.Ticker} {tradeQty} 개 체결됨, 가격: {matchPrice}원");

                // 시스템 주문(UserId=0)은 orders 테이블에 없으므로 trade 기록 제외
                if (incomingOrder.UserId != 0 && makerOrder.UserId != 0)
                {
                    _tradeChannel.Writer.TryWrite(new TradeReceipt()
                    {
                        Ticker = incomingOrder.Ticker,
                        Price = matchPrice,
                        Quantity = tradeQty,
                        BuyerId = (incomingOrder.Side == OrderSide.Buy) ? incomingOrder.OrderId : makerOrder.OrderId,
                        SellerId = (incomingOrder.Side == OrderSide.Buy) ? makerOrder.OrderId : incomingOrder.OrderId,
                        TradeTime = DateTime.Now
                    });

                    long buyerUserId  = incomingOrder.Side == OrderSide.Buy ? incomingOrder.UserId : makerOrder.UserId;
                    long sellerUserId = incomingOrder.Side == OrderSide.Buy ? makerOrder.UserId : incomingOrder.UserId;
                    _pushChannel.Writer.TryWrite(BuildTradePush(buyerUserId,  incomingOrder.Ticker, OrderSide.Buy,  matchPrice, tradeQty));
                    _pushChannel.Writer.TryWrite(BuildTradePush(sellerUserId, incomingOrder.Ticker, OrderSide.Sell, matchPrice, tradeQty));
                }

                if (makerOrder.ExecutedQuantity == makerOrder.Quantity)
                {
                    orderQueue.Dequeue();
                }
            }

            if (orderQueue.Count == 0)
            {
                oppsiteBook.Remove(bestPriceLevel.Key);
            }
        }
    }

    // 서버 기동 중(리스너 시작 전)에만 호출된다 — 워커/스케줄러와 경합하지 않으므로 대기열을 거치지 않는다
    public async Task AddExistingOrder(Order order)
    {
        var orderBook = _orderBooks.GetOrAdd(order.Ticker, t => new OrderBook(t));
        orderBook.AddOrder(order);
    }

    // newTargetPrice 범위 안에 있는 호가 레벨의 잔여 수량 합산
    // isAsk=true → Asks 순회 (오름차순, price <= limitPrice)
    // isAsk=false → Bids 순회 (내림차순, price >= limitPrice)
    private int ComputeStrikeQuantity(
        OrderBook orderBook,
        SortedDictionary<long, Queue<Order>> book,
        long limitPrice,
        bool isAsk)
    {
        long total = 0;
        lock (orderBook.SyncRoot)
        {
            foreach (var (levelPrice, queue) in book)
            {
                bool inRange = isAsk ? levelPrice <= limitPrice : levelPrice >= limitPrice;
                if (!inRange) break;
                foreach (var order in queue)
                    total += order.Quantity - order.ExecutedQuantity;
            }
        }
        return (int)Math.Min(total, int.MaxValue);
    }

    private async Task RunValueChainTick()
    {
        // 순회 중 가격 변동 방지를 위해 스냅샷 확보
        Dictionary<string, long> priceSnapshot;
        _priceLock.EnterReadLock();
        try
        {
            priceSnapshot = _tickers.ToDictionary(k => k.Key, v => v.Value.CurrentPrice);
        }
        finally { _priceLock.ExitReadLock(); }

        foreach (var (sourceTicker, relations) in _relations)
        {
            if (!priceSnapshot.TryGetValue(sourceTicker, out long currentPrice)) continue;
            if (!_lastPrices.TryGetValue(sourceTicker, out long lastPrice)) continue;

            long delta = currentPrice - lastPrice;
            if (delta == 0) continue;

            foreach (var rel in relations)
            {
                string targetTicker = rel.TargetAsset;
                if (!_tickers.ContainsKey(targetTicker)) continue;

                long targetCurrentPrice, targetLower, targetUpper;
                _priceLock.EnterReadLock();
                try
                {
                    var info = _tickers[targetTicker];
                    targetCurrentPrice = info.CurrentPrice;
                    targetLower = info.LowerLimitPrice;
                    targetUpper = info.UpperLimitPrice;
                }
                finally { _priceLock.ExitReadLock(); }

                float weight = delta > 0 ? rel.PositiveWeight : rel.NegativeWeight;
                long targetDelta = (long)(delta * weight);
                long newTargetPrice = Math.Clamp(targetCurrentPrice + targetDelta, targetLower, targetUpper);

                if (newTargetPrice == targetCurrentPrice) continue;
                if (!_orderBooks.TryGetValue(targetTicker, out var orderBook)) continue;

                bool goingUp = newTargetPrice > targetCurrentPrice;
                int strikeQty = goingUp
                    ? ComputeStrikeQuantity(orderBook, orderBook.Asks, newTargetPrice, isAsk: true)
                    : ComputeStrikeQuantity(orderBook, orderBook.Bids, newTargetPrice, isAsk: false);

                if (strikeQty == 0) continue;

                var systemOrder = new Order
                {
                    UserId = 0,   // 시스템 주문 — OrderService 우회, 잔액 체크 없음
                    Ticker = targetTicker,
                    Side = goingUp ? OrderSide.Buy : OrderSide.Sell,
                    Price = newTargetPrice,
                    Quantity = strikeQty,
                    ExecutedQuantity = 0,
                    Status = OrderStatus.PENDING,
                    CreateAt = DateTime.UtcNow
                };

                try
                {
                    await ProcessOrder(systemOrder);
                    Console.WriteLine($"[ValueChain] {sourceTicker}→{targetTicker} delta={delta} strikeQty={strikeQty} @ {newTargetPrice}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ValueChain] Strike failed {targetTicker}: {ex.Message}");
                }
            }
        }

        // 틱 끝에 lastPrices 갱신
        _priceLock.EnterWriteLock();
        try
        {
            foreach (var kvp in _tickers)
                _lastPrices[kvp.Key] = kvp.Value.CurrentPrice;
        }
        finally { _priceLock.ExitWriteLock(); }
    }

    // Body: ticker(4) + side(2) + matchPrice(8) + matchQty(4) = 18바이트
    private PushPayload BuildTradePush(long targetUserId, string ticker, OrderSide side, long price, int qty)
    {
        var body = new byte[18];
        Encoding.UTF8.GetBytes(ticker.PadRight(4)).CopyTo(body, 0);
        BitConverter.TryWriteBytes(body.AsSpan(4), (short)(side == OrderSide.Buy ? 1 : 2));
        BitConverter.TryWriteBytes(body.AsSpan(6), price);
        BitConverter.TryWriteBytes(body.AsSpan(14), qty);
        return new PushPayload(0x10, targetUserId, body);
    }

    // Body: ticker(4) + bidCount(4) + bids[price(8)+qty(4)]×N + askCount(4) + asks[price(8)+qty(4)]×M
    private PushPayload BuildOrderBookPush(string ticker, OrderBook orderBook)
    {
        var bids = new List<(long price, int qty)>();
        var asks = new List<(long price, int qty)>();

        lock (orderBook.SyncRoot)
        {
            foreach (var (levelPrice, queue) in orderBook.Bids)
            {
                int total = queue.Sum(o => o.Quantity - o.ExecutedQuantity);
                if (total > 0) bids.Add((levelPrice, total));
            }
            foreach (var (levelPrice, queue) in orderBook.Asks)
            {
                int total = queue.Sum(o => o.Quantity - o.ExecutedQuantity);
                if (total > 0) asks.Add((levelPrice, total));
            }
        }

        int bodyLen = 4 + 4 + bids.Count * 12 + 4 + asks.Count * 12;
        var body = new byte[bodyLen];
        int offset = 0;

        Encoding.UTF8.GetBytes(ticker.PadRight(4)).CopyTo(body, offset); offset += 4;
        BitConverter.TryWriteBytes(body.AsSpan(offset), bids.Count); offset += 4;
        foreach (var (p, q) in bids)
        {
            BitConverter.TryWriteBytes(body.AsSpan(offset), p); offset += 8;
            BitConverter.TryWriteBytes(body.AsSpan(offset), q); offset += 4;
        }
        BitConverter.TryWriteBytes(body.AsSpan(offset), asks.Count); offset += 4;
        foreach (var (p, q) in asks)
        {
            BitConverter.TryWriteBytes(body.AsSpan(offset), p); offset += 8;
            BitConverter.TryWriteBytes(body.AsSpan(offset), q); offset += 4;
        }

        return new PushPayload(0x11, 0, body);
    }

    public async Task StartValueChainScheduler()
    {
        var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));
        while (await timer.WaitForNextTickAsync())
        {
            try { await RunValueChainTick(); }
            catch (Exception ex)
            {
                Console.WriteLine($"[ValueChain] Tick error: {ex.Message}");
            }
        }
    }
}
