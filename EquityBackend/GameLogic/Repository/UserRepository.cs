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
        // users.user_id 는 GENERATED ALWAYS identity 라서 INSERT ... ON CONFLICT 는
        // 행 존재 여부와 상관없이 항상 실패한다(428C9). 존재하는 사용자만 갱신한다.
        const string sql = @"
UPDATE users SET balance = users.balance + @amount
WHERE user_id = @userId;";

        int affected = await _db.ExecuteAsync(sql, new { UserId = userId, Amount = amount }, transaction: _tx);
        if (affected != 1)
            throw new InvalidOperationException($"잔고 갱신 대상 사용자가 없습니다: {userId}");
    }


    public async Task UpdateLockedBalance(long userId, long amount)
    {
        const string sql = @"
UPDATE users SET locked_balance = users.locked_balance + @amount
WHERE user_id = @userId;";

        int affected = await _db.ExecuteAsync(sql, new { UserId = userId, Amount = amount }, transaction: _tx);
        if (affected != 1)
            throw new InvalidOperationException($"묶인 잔고 갱신 대상 사용자가 없습니다: {userId}");
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