using System.Collections;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using Npgsql;

namespace EquityBackend.GameLogic;
using System;
using System.Collections.Generic;

public enum OrderSide { Buy, Sell }
public enum OrderStatus { PENDING, PARTIAL, COMPLETED, CANCELED }

public class Order
{
    public long OrderId { get; set; } // 고유 주문 번호
    public long UserId { get; set; } // 주문한 유저 ID
    public string Ticker { get; set; } // 중복 코드
    public OrderSide Side { get; set; } // 매수/매도
    public long Price { get; set; } //가격
    public int Quantity { get; set; } //총 주문량 
    public int ExecutedQuantity { get; set; } //채결된 주문량
    public OrderStatus Status { get; set; } //상태
    public DateTime CreateAt { get; set; } //타임스탬프
}

public class OrderRepository
{
    private readonly string _connectionString;
    private IDbConnection _db;
    private IDbTransaction _trx;
    
    public OrderRepository(string connectionString, IDbConnection db, IDbTransaction trx)
    {
        _connectionString = connectionString;
        _db = db;
        _trx = trx;
    }

    
    //데이터 넣기
    public async Task<long> InsertOrderAsync(Order order)
    {
        const string sql = @"
    INSERT INTO orders (user_id, ticker, side, price, quantity, executed_qty, status)
VALUES (@UserId, @Ticker, @Side, @Price, @Quantity, 0, @Status)
RETURNING order_id;";

            //SQL @UserId에 자동 매핑
            var parameters = new
            {
                UserId = order.UserId,
                Ticker = order.Ticker,
                Side = order.Side.ToString(),
                Price = order.Price,
                Quantity = order.Quantity,
                Status = order.Status.ToString(),
                CreateAt = order.CreateAt
            };
            return await _db.QuerySingleAsync<long>(sql, parameters, _trx);
    }
    
    //미채결 조회
    public async Task<IEnumerable<Order>> GetPendingOrderAsync(long userId)
    {
        const string sql = @"
SELECT * FROM Orders
WHERE user_id = @UserId AND status = 'PENDING';"; 
        return await _db.QueryAsync<Order>(sql, new { UserId = userId });
    }
    
    public async Task<IEnumerable<Order>> GetAllPendingOrdersAsync()
    {
        const string sql = @"
        SELECT * FROM Orders 
        WHERE status = 'PENDING' 
        ORDER BY created_at ASC;"; // 먼저 주문한 사람이 우선권을 가져야 하니까요!
    
        // 이 메서드는 서버 켜질 때 딱 한 번 쓰이므로 _trx(트랜잭션) 없이 
        // 기본 연결(_db)만 사용해도 무방합니다.
        return await _db.QueryAsync<Order>(sql);
    }

}