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
    private readonly Dictionary<string, OrderBook> _orderBooks = new Dictionary<string, OrderBook>();
    private Dictionary<string, AssetTickerInfo> _tickers = new Dictionary<string, AssetTickerInfo>();

    // 현재가 읽기/쓰기 보호
    private readonly ReaderWriterLockSlim _priceLock = new ReaderWriterLockSlim();

    public ChannelReader<TradeReceipt> TradeReader => _tradeChannel.Reader;

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

    public async Task ProcessOrder(Order incomingOrder)
    {
        AssetTickerInfo assetTickerInfo = _tickers[incomingOrder.Ticker];

        if (assetTickerInfo.LowerLimitPrice > incomingOrder.Price ||
            assetTickerInfo.UpperLimitPrice < incomingOrder.Price)
        {
            throw new Exception("Asset ticker is out of range");
        }

        //호가창이 없다면 새로 만든다
        if (!_orderBooks.ContainsKey(incomingOrder.Ticker))
        {
            _orderBooks[incomingOrder.Ticker] = new OrderBook(incomingOrder.Ticker);
        }
        var orderBook = _orderBooks[incomingOrder.Ticker];

        MatchOrder(incomingOrder, orderBook);

        if (incomingOrder.Quantity > incomingOrder.ExecutedQuantity)
        {
            orderBook.AddOrder(incomingOrder);
            Console.WriteLine($"[호가 등록] {incomingOrder.Ticker} {incomingOrder.Side} 대기 등록 - 잔여수량: {incomingOrder.Quantity - incomingOrder.ExecutedQuantity}개");
        }
    }

    public void MatchOrder(Order incomingOrder, OrderBook orderBook)
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

    public async Task AddExistingOrder(Order order)
    {
        if (!_orderBooks.ContainsKey(order.Ticker))
        {
            _orderBooks[order.Ticker] = new OrderBook(order.Ticker);
        }

        var orderBook = _orderBooks[order.Ticker];
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
