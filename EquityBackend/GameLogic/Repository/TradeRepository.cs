using System.Data;
using Dapper;

namespace EquityBackend.GameLogic;

public class TradeRepository
{
    private readonly string _connectionString;
    private readonly IDbConnection _db;
    private readonly IDbTransaction _trx;
    
    public TradeRepository(string connectionString, IDbConnection db, IDbTransaction trx)
    {
        _connectionString = connectionString;
        _db = db;
        _trx = trx;
    }

    // 실제 테이블 이름은 trades (기존 코드의 Trade 테이블은 존재하지 않아 INSERT가 항상 실패했다)
    public async Task InsertTaskAsync(TradeReceipt tradeReceipt)
    {
        const string sql = @"INSERT INTO trades (ticker, buy_order_id, sell_order_id, match_price, match_qty, trade_at)
        VALUES (@ticker, @buyOrderId, @sellOrderId, @price, @amount, @tradeAt);";

        await _db.ExecuteAsync(sql, new {Ticker = tradeReceipt.Ticker, BuyOrderId = tradeReceipt.BuyOrderId,
            SellOrderId = tradeReceipt.SellOrderId, Price = tradeReceipt.Price, Amount = tradeReceipt.Quantity,
            TradeAt = tradeReceipt.TradeTime}, _trx);
    }

    // 종목별 마지막 체결가 (trade_id가 가장 큰 체결)
    public async Task<Dictionary<string, long>> GetLastPricesAsync()
    {
        const string sql = @"
SELECT DISTINCT ON (ticker) ticker AS Ticker, match_price AS Price
FROM trades
ORDER BY ticker, trade_id DESC;";
        var rows = await _db.QueryAsync<(string Ticker, long Price)>(sql, transaction: _trx);
        return rows.ToDictionary(r => r.Ticker, r => r.Price);
    }

    // 주문 ID별 체결 수량 합계 (매수/매도 양쪽 컬럼 모두)
    public async Task<Dictionary<long, int>> GetExecutedQtyByOrderAsync(long[] orderIds)
    {
        const string sql = @"
SELECT order_id AS OrderId, SUM(match_qty)::int AS Qty
FROM (
    SELECT buy_order_id AS order_id, match_qty FROM trades WHERE buy_order_id = ANY(@Ids)
    UNION ALL
    SELECT sell_order_id, match_qty FROM trades WHERE sell_order_id = ANY(@Ids)
) t
GROUP BY order_id;";
        var rows = await _db.QueryAsync<(long OrderId, int Qty)>(sql, new { Ids = orderIds }, _trx);
        return rows.ToDictionary(r => r.OrderId, r => r.Qty);
    }
}