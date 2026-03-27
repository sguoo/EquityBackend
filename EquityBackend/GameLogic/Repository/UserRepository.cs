using System.Data;
using Dapper;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace EquityBackend.GameLogic;

public class UserRepository
{
    private readonly string _connectionString;
    private readonly IDbConnection _db;
    private readonly IDbTransaction _tx;
    
    public UserRepository(string connectionString, IDbConnection db, IDbTransaction tx)
    {
        _connectionString = connectionString;
        _db = db;
        _tx = tx;
        
    }

    public async Task NewUserInsert(long userId, string userName)
    {
        const string sql = @"
INSERT INTO users (user_id, user_name)
VALUES (@userId, @userName);";
    }

    public async Task UpdateBalance(long userId,long amount)
    {
        const string sql = @"
INSERT INTO users (user_id, balance)
VALUES (@userId, @amount)

ON CONFLICT (user_id)

DO UPDATE
SET balance = users.balance + @amount;";
        
        await _db.ExecuteAsync(sql, new { UserId = userId, Amount = amount }, transaction: _tx);
    }


    public async Task UpdateLockedBalance(long userId, long amount)
    {
        const string sql = @"
UPDATE users SET locked_balance = users.locked_balance + @amount
WHERE user_id = @userId;";

        await _db.ExecuteAsync(sql, new { UserId = userId, Amount = amount }, transaction: _tx);
    }

    public async Task<long> GetUserBalance(long userId)
    {
        const string sql = @"
SELECT balance FROM users WHERE user_id = @userId;";
        
        return await _db.QueryFirstOrDefaultAsync<long>(sql, new { UserId = userId }, transaction: _tx);
    }

    private IDbConnection CreateConnection()
    {
        return new NpgsqlConnection(_connectionString);
    }
    
    public async Task AddDailyInterestAsync(decimal dailyInterest)
    {
        string sql = @"
UPDATE users SET balance = CAST(balance * @dailyInterest AS BIGINT)";
        await _db.ExecuteAsync(sql, new { DailyInterest = dailyInterest }, transaction: _tx);
    }

}