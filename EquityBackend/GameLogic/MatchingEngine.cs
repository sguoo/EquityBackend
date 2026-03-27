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
    private readonly Channel<TradeReceipt> _tradeChannel = Channel.CreateUnbounded<TradeReceipt>();
    private readonly Dictionary<string, OrderBook> _orderBooks = new Dictionary<string, OrderBook>();
    private Dictionary<string, AssetTickerInfo> _tickers = new Dictionary<string, AssetTickerInfo>();
    
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
            //가장 싼 가격이 내가 부른 가격보다 비싸면 실패
            if (incomingOrder.Side == OrderSide.Buy && matchPrice > incomingOrder.Price) break;
            //가장 비싼 가격이 내가 팔려는 가격보다 쌀시 실패
            if (incomingOrder.Side == OrderSide.Sell && matchPrice < incomingOrder.Price) break;

            while (incomingOrder.ExecutedQuantity < incomingOrder.Quantity && orderQueue.Count > 0)
            {
                //가장 먼저온 주문 확인
                var makerOrder = orderQueue.Peek();
                
                //체결 가능한 수량 확인
                int incomingRemaining = incomingOrder.Quantity - incomingOrder.ExecutedQuantity;
                int makerRemaining = makerOrder.Quantity - makerOrder.ExecutedQuantity;
                int tradeQty = Math.Min(incomingRemaining, makerRemaining);

                incomingOrder.ExecutedQuantity += tradeQty;
                makerOrder.ExecutedQuantity += tradeQty;
                _tickers[incomingOrder.Ticker].CurrentPrice = matchPrice;
                Console.WriteLine($"채결 완료: {incomingOrder.Ticker} {tradeQty} 개 체결됨, 가격: {matchPrice}원");
                
                // DB trades 테이블에 insert
                _tradeChannel.Writer.TryWrite(new TradeReceipt()
                {
                    Ticker = incomingOrder.Ticker,
                    Price = matchPrice,
                    Quantity = tradeQty,
                    BuyerId = (incomingOrder.Side == OrderSide.Buy) ? incomingOrder.OrderId : makerOrder.OrderId,
                    SellerId = (incomingOrder.Side == OrderSide.Buy) ? makerOrder.OrderId : incomingOrder.OrderId,
                    TradeTime = DateTime.Now
                });
                
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
        orderBook.AddOrder(order);    }
    
}