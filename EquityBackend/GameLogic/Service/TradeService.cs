namespace EquityBackend.GameLogic.Service;

// 체결 영수증 1건을 DB에 반영한다. 매칭 워커가 아니라 체결 채널을 읽는 백그라운드 작업에서만 호출된다.
public class TradeService
{
    private readonly DbManager _dbManager;

    public TradeService(DbManager dbManager)
    {
        _dbManager = dbManager;
    }

    // 체결 1건 = 트랜잭션 1개. 하나라도 실패하면 전부 롤백하고 예외를 다시 던진다.
    //  1) trades INSERT
    //  2) 사용자 주문의 executed_qty 증가 + PARTIAL/COMPLETED
    //  3) 정산 — 매수자: 묶인 금액 해제, 체결 금액 지불, 지정가 차액 환불, 보유 수량 지급
    //            매도자: 묶인 보유 수량 차감, 체결 금액 입금
    // 시스템 주문(주문 ID null) 쪽은 2), 3)을 건너뛴다.
    public async Task SettleTradeAsync(TradeReceipt r)
    {
        await _dbManager.DbConnection();
        try
        {
            var asset = await _dbManager._assetsRepo.GetAssetAsync(r.Ticker)
                        ?? throw new AssetNotFoundException();

            // 주문 시 묶을 때와 같은 규칙(OrderService.CalculateAmount)으로 계산한다
            long matchAmount = OrderService.CalculateAmount(r.Price, r.Quantity, asset.IsFractional);

            await _dbManager._tradeRepo.InsertTaskAsync(r);

            if (r.BuyOrderId is long buyOrderId)
            {
                int executedAfter = await _dbManager._orderRepo.ApplyFillAsync(buyOrderId, r.Quantity);
                int executedBefore = executedAfter - r.Quantity;

                // 주문 시 묶은 금액 = CalculateAmount(지정가, 주문수량).
                // 체결분만큼 누적 차이로 해제해야 IsFractional의 정수 나눗셈 반올림이 쌓여도
                // 주문이 다 찼을 때 해제 합계가 묶은 금액과 정확히 같아진다.
                long lockRelease =
                    OrderService.CalculateAmount(r.BuyLimitPrice, executedAfter, asset.IsFractional) -
                    OrderService.CalculateAmount(r.BuyLimitPrice, executedBefore, asset.IsFractional);
                long refund = lockRelease - matchAmount; // 지정가보다 싸게 체결된 차액

                if (refund < 0)
                    throw new InvalidOperationException(
                        $"매수 정산 금액 오류: order_id={buyOrderId}, 해제 {lockRelease} < 체결 {matchAmount}");

                await _dbManager._userRepo.UpdateLockedBalance(r.BuyerUserId, -lockRelease);
                if (refund > 0)
                    await _dbManager._userRepo.UpdateBalance(r.BuyerUserId, refund);
                await _dbManager._inventoryRepo.UpdateInventory(r.BuyerUserId, r.Ticker, r.Quantity);
            }

            if (r.SellOrderId is long sellOrderId)
            {
                await _dbManager._orderRepo.ApplyFillAsync(sellOrderId, r.Quantity);
                await _dbManager._inventoryRepo.UpdateLockedInventory(r.SellerUserId, r.Ticker, -r.Quantity);
                await _dbManager._userRepo.UpdateBalance(r.SellerUserId, matchAmount);
            }

            await _dbManager.CommitAsync();
        }
        catch
        {
            try { await _dbManager.RollbackAsync(); }
            catch (Exception rollbackEx) { Console.WriteLine($"[Trade] 롤백 실패: {rollbackEx.Message}"); }
            throw;
        }
        finally
        {
            await _dbManager.DisposeAsync();
        }
    }
}
