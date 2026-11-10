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

    // 체결 수량 반영 — 다 찼으면 COMPLETED, 아니면 PARTIAL. 갱신 후 executed_qty를 돌려준다.
    // 주문 수량을 넘기는 갱신(중복 체결)은 0행이 되어 예외 → 트랜잭션 롤백.
    public async Task<int> ApplyFillAsync(long orderId, int fillQty)
    {
        const string sql = @"
UPDATE orders
SET executed_qty = executed_qty + @FillQty,
    status = CASE WHEN executed_qty + @FillQty >= quantity THEN 'COMPLETED' ELSE 'PARTIAL' END
WHERE order_id = @OrderId
  AND status IN ('PENDING', 'PARTIAL')
  AND executed_qty + @FillQty <= quantity
RETURNING executed_qty;";

        var executed = await _db.QuerySingleOrDefaultAsync<int?>(sql,
            new { OrderId = orderId, FillQty = fillQty }, _trx);
        return executed ?? throw new InvalidOperationException(
            $"체결 수량 반영 실패: order_id={orderId}, fill={fillQty} (주문 없음/이미 완료/수량 초과)");
    }

    // 재시작 시 trades 합계와 다를 때 trades 기준으로 맞춘다
    public async Task SetExecutedQtyAsync(long orderId, int executedQty)
    {
        const string sql = @"
UPDATE orders
SET executed_qty = @ExecutedQty,
    status = CASE WHEN @ExecutedQty >= quantity THEN 'COMPLETED'
                  WHEN @ExecutedQty > 0 THEN 'PARTIAL'
                  ELSE 'PENDING' END
WHERE order_id = @OrderId;";
        await _db.ExecuteAsync(sql, new { OrderId = orderId, ExecutedQty = executedQty }, _trx);
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