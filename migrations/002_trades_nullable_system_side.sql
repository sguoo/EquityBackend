-- 가치사슬 시스템 주문(UserId = 0)은 orders 테이블에 행이 없다.
-- 사용자 주문과 시스템 주문이 체결되면 시스템 쪽 주문 ID를 NULL로 기록한다.
ALTER TABLE trades ALTER COLUMN buy_order_id DROP NOT NULL;
ALTER TABLE trades ALTER COLUMN sell_order_id DROP NOT NULL;

-- 양쪽 모두 시스템 주문인 체결은 기록하지 않는다
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'trades_has_user_order') THEN
        ALTER TABLE trades ADD CONSTRAINT trades_has_user_order
            CHECK (buy_order_id IS NOT NULL OR sell_order_id IS NOT NULL);
    END IF;
END $$;

-- 재시작 시: 주문별 체결 수량 합산, 종목별 마지막 체결가 조회
CREATE INDEX IF NOT EXISTS idx_trades_buy_order_id  ON trades (buy_order_id);
CREATE INDEX IF NOT EXISTS idx_trades_sell_order_id ON trades (sell_order_id);
CREATE INDEX IF NOT EXISTS idx_trades_ticker_trade_id ON trades (ticker, trade_id);
