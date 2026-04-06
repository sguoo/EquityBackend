namespace EquityBackend.GameLogic;

public class OrderBook
{
    public string Ticker { get; set; }

    // 비싸게 사는 사람이 1순위
    public SortedDictionary<long, Queue<Order>> Bids { get; set; } =
        new SortedDictionary<long, Queue<Order>>(Comparer<long>.Create((x, y) => y.CompareTo(x)));

    // 싸게 팔 사람이 1순위
    public SortedDictionary<long, Queue<Order>> Asks { get; } =
        new SortedDictionary<long, Queue<Order>>();

    // 락
    private readonly object _lock = new object();
    public object SyncRoot => _lock;

    public OrderBook(string ticker)
    {
        Ticker = ticker;
    }

    public void AddOrder(Order order)
    {
        lock (_lock)
        {
            var targetBook = order.Side == OrderSide.Buy ? Bids : Asks;

            if (!targetBook.ContainsKey(order.Price))
            {
                targetBook[order.Price] = new Queue<Order>();
            }

            targetBook[order.Price].Enqueue(order);
        }
    }
}
