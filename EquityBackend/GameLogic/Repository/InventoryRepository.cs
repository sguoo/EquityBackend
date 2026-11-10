using System.Data;
using Dapper;

namespace EquityBackend.GameLogic;

public class InventoryRepository
{
    private readonly string _connectionString;
    private readonly IDbConnection _db;
    private readonly IDbTransaction _trx;

    public InventoryRepository(string connectionString, IDbConnection db, IDbTransaction trx)
    {
        _connectionString = connectionString;
        _db = db;
        _trx = trx;
    }
    public async Task UpdateInventory(long userId, string ticker, int amount) { 
        const string sql = @"
INSERT INTO Inventory (user_id, ticker, quantity)
VALUES (@userId, @ticker, @amount)

ON CONFLICT (user_id, ticker)

DO UPDATE
SET quantity = Inventory.quantity + @amount;";
        
        await _db.ExecuteAsync(sql, new {UserId = userId, Ticker = ticker, Amount =  amount}, transaction: _trx);
    }
    public async Task UpdateLockedInventory(long userId, string ticker, int amount) { 
             const string sql = @"
UPDATE Inventory SET locked_quantity = Inventory.locked_quantity + @amount
WHERE user_id = @userId AND ticker = @ticker;";
             
             int affected = await _db.ExecuteAsync(sql, new {UserId = userId, Ticker = ticker, Amount =  amount}, transaction: _trx);
             if (affected != 1)
                 throw new InvalidOperationException($"묶인 보유 수량 갱신 대상이 없습니다: {userId}/{ticker}");
    }
public async Task<int> GetInventoryQuantity(long userId, string ticker)
    {
        const string sql = @"
SELECT quantity FROM inventory WHERE user_id = @userId AND ticker = @ticker;";
        
        return await _db.QueryFirstOrDefaultAsync<int>(sql, new { UserId = userId, ticker = ticker }, transaction: _trx);
    }
}