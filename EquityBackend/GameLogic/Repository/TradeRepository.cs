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

    public async Task InsertTaskAsync(TradeReceipt tradeReceipt)
    {
        const string sql = @"INSERT INTO Trade (ticker, buy_order_id, sell_order_id, match_price, match_qty) 
        VALUES (@ticker, @buyOrderId, @sellOrderId, @price, @amount);";
        
        await _db.ExecuteAsync(sql, new {Ticker = tradeReceipt.Ticker, BuyOrderId = tradeReceipt.BuyerId, 
            SellOrderId = tradeReceipt.SellerId, Price = tradeReceipt.Price, Amount = tradeReceipt.Quantity}, _trx);
    }
}