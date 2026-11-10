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
}