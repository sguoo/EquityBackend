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
    
    // SELECT * 는 executed_qty / created_at 이 속성 이름과 달라 매핑되지 않으므로 별칭을 명시한다
    private const string OrderColumns = @"
    order_id     AS OrderId,
    user_id      AS UserId,
    ticker       AS Ticker,
    side         AS Side,
    price        AS Price,
    quantity     AS Quantity,
    executed_qty AS ExecutedQuantity,
    status       AS Status,
    created_at   AS CreateAt";

    //미채결 조회 (일부 체결 포함)
    public async Task<IEnumerable<Order>> GetPendingOrderAsync(long userId)
    {
        const string sql = "SELECT" + OrderColumns + @"
FROM orders
WHERE user_id = @UserId AND status IN ('PENDING', 'PARTIAL');";
        return await _db.QueryAsync<Order>(sql, new { UserId = userId }, _trx);
    }

    // 재시작 시 호가창 복원용 — 일부 체결(PARTIAL) 주문도 남은 수량만큼 다시 올라가야 한다
    public async Task<IEnumerable<Order>> GetAllPendingOrdersAsync()
    {
        const string sql = "SELECT" + OrderColumns + @"
FROM orders
WHERE status IN ('PENDING', 'PARTIAL')
ORDER BY created_at ASC, order_id ASC;"; // 시간 우선 — 같은 시각이면 먼저 INSERT된 주문 우선

        return await _db.QueryAsync<Order>(sql, transaction: _trx);
    }

}