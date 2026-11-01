using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using EquityBackend.GameLogic;
using EquityBackend.GameLogic.Service;

class Program
{
    static async Task Main(string[] args)
    {

        TcpListener server = null;

        try
        {
            string connectionString = "Host=localhost;Username=postgres;Password=1020;Database=Equity";

            var _dbManager = new DbManager();
            await _dbManager.DbConnection();
            var orderService = new OrderService(_dbManager);
            var pendingOrders = await _dbManager._orderRepo.GetAllPendingOrdersAsync();
            var orderRepo = _dbManager._orderRepo;

            var engine = new MatchingEngine();
            engine.InitializeTickers(await _dbManager._assetsRepo.GetAssetsAsync());
            engine.InitializeRelations(await _dbManager._assetsRepo.GetAllRelationsAsync());
            engine.InitializeLastPrices();

            foreach (var order in pendingOrders)
            {
                await engine.AddExistingOrder(order);
            }

            // userId → NetworkStream 레지스트리 (Push 대상 관리)
            var clients = new ConcurrentDictionary<long, NetworkStream>();

            int port = 8080;
            IPAddress localAddr = IPAddress.Parse("127.0.0.1");

            server = new TcpListener(localAddr, port);

            server.Start();

            _ = Task.Run(async () =>
            {
                await foreach (var receipt in engine.TradeReader.ReadAllAsync())
                {
                    var tradeDbManager = new DbManager();
                    try
                    {
                        await tradeDbManager.DbConnection();
                        await tradeDbManager._tradeRepo.InsertTaskAsync(receipt);

                        await tradeDbManager.CommitAsync();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"영수증 오류: {e.Message}");
                        await tradeDbManager.RollbackAsync();
                    }
                    finally
                    {
                        await tradeDbManager.DisposeAsync();
                    }
                }
            });

            _ = engine.StartValueChainScheduler();

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

            while (true)
            {
                Console.WriteLine("Waiting for a connection...");

                TcpClient client = await server.AcceptTcpClientAsync();
                Console.WriteLine("Connection accepted");

                _ = Task.Run((() => HandleClientAsync(client, engine, clients)));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        finally
        {
            server.Stop();
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
