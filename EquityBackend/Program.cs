using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using EquityBackend.GameLogic;
using EquityBackend.GameLogic.Service;

// 정상 종료 시 새 주문을 막고, 이미 처리 중인 주문(OrderService → ProcessOrder)이 끝나기를 기다린다.
// TryEnter는 "증가 후 확인" 순서라 Close 이후에 통과하는 주문이 없다.
sealed class OrderGate
{
    private int _inFlight;
    private int _closed;

    public bool TryEnter()
    {
        Interlocked.Increment(ref _inFlight);
        if (Volatile.Read(ref _closed) == 1)
        {
            Interlocked.Decrement(ref _inFlight);
            return false;
        }
        return true;
    }

    public void Exit() => Interlocked.Decrement(ref _inFlight);

    public async Task CloseAndDrainAsync(TimeSpan timeout)
    {
        Interlocked.Exchange(ref _closed, 1);
        var deadline = DateTime.UtcNow + timeout;
        while (Volatile.Read(ref _inFlight) > 0 && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        if (Volatile.Read(ref _inFlight) > 0)
            Console.WriteLine($"[Shutdown][경고] 처리 중 주문 {_inFlight}건을 기다리다 시간 초과");
    }
}

class Program
{
    static readonly OrderGate OrderGate = new OrderGate();

    static async Task Main(string[] args)
    {

        TcpListener? server = null;

        // Ctrl+C(SIGINT) / SIGTERM / Ctrl+Break(SIGQUIT) → 정상 종료 절차
        using var shutdownCts = new CancellationTokenSource();
        using var shutdownDone = new ManualResetEventSlim(false);
        void OnSignal(PosixSignalContext ctx)
        {
            ctx.Cancel = true; // 프로세스 즉시 종료를 막고 Main이 정리하게 한다
            Console.WriteLine($"[Shutdown] {ctx.Signal} 수신 — 종료 절차 시작");
            shutdownCts.Cancel();
            // Windows의 창 닫기/로그오프(SIGTERM)는 핸들러가 반환되면 곧바로 종료되므로 정리를 기다린다
            if (ctx.Signal == PosixSignal.SIGTERM)
                shutdownDone.Wait(TimeSpan.FromSeconds(4));
        }
        using var sigInt  = PosixSignalRegistration.Create(PosixSignal.SIGINT,  OnSignal);
        using var sigTerm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, OnSignal);
        using var sigQuit = PosixSignalRegistration.Create(PosixSignal.SIGQUIT, OnSignal);

        try
        {
            string connectionString = "Host=localhost;Username=postgres;Password=1020;Database=Equity";

            var engine = new MatchingEngine();
            await RestoreEngineAsync(engine);

            // userId → NetworkStream 레지스트리 (Push 대상 관리)
            var clients = new ConcurrentDictionary<long, NetworkStream>();

            int port = 8080;
            IPAddress localAddr = IPAddress.Parse("127.0.0.1");

            server = new TcpListener(localAddr, port);

            server.Start();

            // 체결 기록 + 정산 — 영수증 1건당 트랜잭션 1개 (TradeService.SettleTradeAsync)
            // 종료 시 채널이 닫히면 남은 영수증을 모두 쓴 뒤 끝난다
            var tradePersistTask = Task.Run(async () =>
            {
                await foreach (var receipt in engine.TradeReader.ReadAllAsync())
                {
                    await PersistTradeAsync(receipt);
                }
            });

            var valueChainTask = engine.StartValueChainScheduler(shutdownCts.Token);

            // Push 브로드캐스트 워커
            _ = Task.Run(async () =>
            {
                await foreach (var payload in engine.PushReader.ReadAllAsync())
                {
                    // header: pushType(2) + bodyLen(4) = 6바이트
                    var frame = new byte[6 + payload.Body.Length];
                    BitConverter.TryWriteBytes(frame.AsSpan(0), payload.Type);
                    BitConverter.TryWriteBytes(frame.AsSpan(2), payload.Body.Length);
                    payload.Body.CopyTo(frame, 6);

                    if (payload.TargetUserId == 0)
                    {
                        foreach (var (_, stream) in clients)
                            await TrySendAsync(stream, frame);
                    }
                    else if (clients.TryGetValue(payload.TargetUserId, out var stream))
                    {
                        await TrySendAsync(stream, frame);
                    }
                }
            });

            try
            {
                while (true)
                {
                    Console.WriteLine("Waiting for a connection...");

                    TcpClient client = await server.AcceptTcpClientAsync(shutdownCts.Token);
                    Console.WriteLine("Connection accepted");

                    _ = Task.Run((() => HandleClientAsync(client, engine, clients)));
                }
            }
            catch (OperationCanceledException) when (shutdownCts.IsCancellationRequested) { }

            // ── 정상 종료 ──
            // 1) 새 연결 차단
            server.Stop();
            // 2) 새 주문 차단 + 이미 들어온 주문은 매칭까지 끝나기를 기다림
            await OrderGate.CloseAndDrainAsync(TimeSpan.FromSeconds(10));
            // 3) 가치사슬 시스템 주문 중단
            await valueChainTask;
            // 4) 종목 대기열을 닫고 워커가 남은 주문을 처리하게 한 뒤 체결 채널을 닫음
            await engine.ShutdownAsync();
            // 5) 체결 채널에 남은 영수증을 전부 DB에 기록
            await tradePersistTask;
            Console.WriteLine("[Shutdown] 남은 체결 기록 완료 — 종료");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        finally
        {
            server?.Stop();
            shutdownDone.Set();
        }

    }

    // 재시작 복원 — TCP 리스너 시작 전에 끝나야 한다.
    //  1) 자산 로드  2) 현재가 = 마지막 체결가(없으면 기준가)
    //  3) PENDING/PARTIAL 주문을 created_at 오름차순으로 조회
    //  4) executed_qty를 trades 합계와 대조 — 다르면 경고 후 trades 기준으로 맞춤
    //  5) 남은 수량으로 호가창에 등록
    static async Task RestoreEngineAsync(MatchingEngine engine)
    {
        var db = new DbManager();
        await db.DbConnection();
        try
        {
            engine.InitializeTickers(await db._assetsRepo.GetAssetsAsync());
            engine.InitializeRelations(await db._assetsRepo.GetAllRelationsAsync());

            var lastPrices = await db._tradeRepo.GetLastPricesAsync();
            engine.RestoreCurrentPrices(lastPrices);
            engine.InitializeLastPrices();
            foreach (var (ticker, price) in lastPrices)
                Console.WriteLine($"[Restore] {ticker} 현재가 = 마지막 체결가 {price}");

            var openOrders = (await db._orderRepo.GetAllPendingOrdersAsync()).ToList();
            var tradedQty = await db._tradeRepo.GetExecutedQtyByOrderAsync(
                openOrders.Select(o => o.OrderId).ToArray());

            int restored = 0;
            foreach (var order in openOrders)
            {
                int fromTrades = tradedQty.GetValueOrDefault(order.OrderId);
                if (fromTrades != order.ExecutedQuantity)
                {
                    Console.WriteLine(
                        $"[Restore][경고] order_id={order.OrderId} executed_qty={order.ExecutedQuantity} " +
                        $"≠ trades 합계 {fromTrades} — trades 기준으로 맞춤");
                    order.ExecutedQuantity = Math.Min(fromTrades, order.Quantity);
                    await db._orderRepo.SetExecutedQtyAsync(order.OrderId, order.ExecutedQuantity);
                }

                if (order.ExecutedQuantity >= order.Quantity)
                    continue; // trades 기준으로 이미 다 찬 주문은 호가창에 올리지 않는다

                await engine.AddExistingOrder(order);
                restored++;
                Console.WriteLine(
                    $"[Restore] order_id={order.OrderId} {order.Ticker} {order.Side} @{order.Price} " +
                    $"잔여 {order.Quantity - order.ExecutedQuantity}/{order.Quantity}");
            }
            Console.WriteLine($"[Restore] 미체결 주문 {restored}건 호가창 복원");

            await db.CommitAsync();
        }
        catch
        {
            await db.RollbackAsync();
            throw;
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    // 일시적 DB 오류(연결 끊김 등)에 대비해 몇 번 재시도한다. 실패한 시도는 트랜잭션이 롤백되므로
    // 일부만 반영된 상태로 남지 않는다. (커밋 성공 후 응답만 유실된 경우의 재시도는 막지 못한다 — 보고서 참고)
    static async Task PersistTradeAsync(TradeReceipt receipt)
    {
        const int maxAttempts = 3;
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await new TradeService(new DbManager()).SettleTradeAsync(receipt);
                return;
            }
            catch (Exception e)
            {
                if (attempt >= maxAttempts)
                {
                    // 여기까지 오면 메모리 호가창과 DB가 어긋난다 — 수동 복구할 수 있도록 영수증 전체를 남긴다
                    Console.WriteLine(
                        $"[Trade] 체결 저장 최종 실패: ticker={receipt.Ticker} price={receipt.Price} qty={receipt.Quantity} " +
                        $"buyOrder={receipt.BuyOrderId?.ToString() ?? "SYSTEM"} sellOrder={receipt.SellOrderId?.ToString() ?? "SYSTEM"} " +
                        $"buyer={receipt.BuyerUserId} seller={receipt.SellerUserId} at={receipt.TradeTime:O} — {e.Message}");
                    return;
                }
                Console.WriteLine($"[Trade] 체결 저장 실패({attempt}/{maxAttempts}), 재시도: {e.Message}");
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt));
            }
        }
    }

    static async Task TrySendAsync(NetworkStream stream, byte[] frame)
    {
        try { await stream.WriteAsync(frame); }
        catch { /* 끊긴 클라이언트는 무시 — 레지스트리 제거는 HandleClientAsync에서 처리 */ }
    }

    static async Task HandleClientAsync(TcpClient client, MatchingEngine engine,
        ConcurrentDictionary<long, NetworkStream> clients)
    {
        Byte[] bytes = new Byte[1024];
        NetworkStream stream = client.GetStream();
        long registeredUserId = 0;

        try
        {
        while (true)
        {
            try
            {
                await stream.ReadExactlyAsync(bytes, 0, 22);

                //명령어 (1=매수, 2=매도, 3=취소)
                short type = BitConverter.ToInt16(bytes, 0);
                //userId
                long userId = BitConverter.ToInt64(bytes, 2);

                // 첫 패킷에서 클라이언트 등록
                if (registeredUserId == 0 && userId != 0)
                {
                    registeredUserId = userId;
                    clients[userId] = stream;
                }
                //ticker (종목)
                string ticker = Encoding.UTF8.GetString(bytes, 10, 4);
                //수량
                int quantity = BitConverter.ToInt32(bytes, 14);
                //가격
                int price = BitConverter.ToInt32(bytes, 18);

                byte[]? returnByte;

                // 종료 절차가 시작되면 DB에 주문을 쓰기 전에 거부한다 (기존 응답 코드 500 사용)
                bool isOrder = type == 1 || type == 2;
                if (isOrder && !OrderGate.TryEnter())
                {
                    Console.WriteLine("[Shutdown] 종료 중 — 새 주문 거부");
                    await stream.WriteAsync(BitConverter.GetBytes(500));
                    continue;
                }

                try
                {
                // 200 = 정상, 400 = 잔액(재화) 부족, 401 = 다른 바이트 값)
                switch (type)
                {
                    case 1:
                        try
                        {
                            var dbManager = new DbManager();
                            var orderService = new OrderService(dbManager);
                            returnByte = BitConverter.GetBytes(200);
                            await engine.ProcessOrder(
                                await orderService.CreateBuyOrder(userId, ticker, quantity, price));
                        }
                        catch (AssetNotFoundException)
                        {
                            Console.WriteLine("Asset not found");
                            returnByte = BitConverter.GetBytes(404);
                        }
                        catch (InsufficientFundsException)
                        {
                            Console.WriteLine("Insufficient funds");
                            returnByte = BitConverter.GetBytes(400);
                        }
                        catch (TickSizeException)
                        {
                            Console.WriteLine("Tick size exception");
                            returnByte = BitConverter.GetBytes(406);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            returnByte = BitConverter.GetBytes(500); // 500 Server Error
                        }

                        await stream.WriteAsync(returnByte);
                        break;
                    case 2:
                        try
                        {
                            var dbManager = new DbManager();
                            var orderService = new OrderService(dbManager);
                            returnByte = BitConverter.GetBytes(200);
                            await engine.ProcessOrder(await orderService.CreateSellOrder(userId, ticker, quantity, price));
                            await stream.WriteAsync(returnByte);
                            break;
                        }
                        catch (Exception e)
                        {

                            Console.WriteLine($"오류: {e.Message}");
                            //잔액부족
                            if (e.Message.Contains("존재하지 않는 종목"))
                            {
                                returnByte = BitConverter.GetBytes(404); // 404 Not Found
                            }
                            else if (e.Message == "잔고 부족!")
                            {
                                returnByte = BitConverter.GetBytes(400); // 400 Bad Request
                            }
                            else if (e.Message == "호가 단위가 맞지 않습니다!")
                            {
                                returnByte = BitConverter.GetBytes(406); // 406 Not Acceptable
                            }
                            else
                            {
                                returnByte = BitConverter.GetBytes(500); // 500 Server Error
                            }
                            await stream.WriteAsync(returnByte);
                            break;
                        }
                    default:
                        //없는 타입
                        returnByte = BitConverter.GetBytes(401);
                        await stream.WriteAsync(returnByte);
                        throw new Exception("Unknown type");
                }
                }
                finally
                {
                    if (isOrder) OrderGate.Exit();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                break;
            }
        }
        } // try
        finally
        {
            if (registeredUserId != 0)
                clients.TryRemove(registeredUserId, out _);
            client.Close();
        }
    }
}
