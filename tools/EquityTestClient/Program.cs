// 검증용 콘솔 도구
//   order <buy|sell> <userId> <ticker> <qty> <price>  — 22바이트 주문 패킷 전송 후 응답 코드 출력
//   sql "<query>"                                    — DB 조회/실행 결과를 표로 출력
//   ctrlc <pid>                                      — 대상 프로세스 콘솔에 Ctrl+C 전송 (정상 종료 검증용, Windows)
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Npgsql;

const string ConnStr = "Host=localhost;Username=postgres;Password=1020;Database=Equity";

if (args.Length == 0)
{
    Console.WriteLine("usage: order <buy|sell> <userId> <ticker> <qty> <price> | sql \"<query>\"");
    return 1;
}

switch (args[0])
{
    case "order":
        return await SendOrder(args[1], long.Parse(args[2]), args[3], int.Parse(args[4]), int.Parse(args[5]));
    case "sql":
        return await RunSql(args[1]);
    case "ctrlc":
        return SendCtrlC(uint.Parse(args[1]));
    default:
        Console.WriteLine($"unknown command: {args[0]}");
        return 1;
}

static async Task<int> SendOrder(string side, long userId, string ticker, int qty, int price)
{
    var packet = new byte[22];
    BitConverter.TryWriteBytes(packet.AsSpan(0), (short)(side == "buy" ? 1 : 2));
    BitConverter.TryWriteBytes(packet.AsSpan(2), userId);
    Encoding.UTF8.GetBytes(ticker.PadRight(4)[..4]).CopyTo(packet, 10);
    BitConverter.TryWriteBytes(packet.AsSpan(14), qty);
    BitConverter.TryWriteBytes(packet.AsSpan(18), price);

    using var client = new TcpClient();
    await client.ConnectAsync("127.0.0.1", 8080);
    var stream = client.GetStream();
    await stream.WriteAsync(packet);

    // 응답(4바이트 int)과 푸시 프레임(type 2 + len 4 + body)이 섞여 올 수 있다.
    // 푸시 type은 0x10/0x11이고 응답 코드(200/400/...)의 하위 2바이트와 겹치지 않는다.
    // 응답을 받은 뒤에도 0.5초 동안은 뒤늦게 도착하는 푸시를 더 읽는다.
    var head = new byte[2];
    int? code = null;
    using var drain = new CancellationTokenSource();
    while (true)
    {
        try { await stream.ReadExactlyAsync(head, drain.Token); }
        catch (OperationCanceledException) { break; }
        catch (EndOfStreamException) { break; }
        short t = BitConverter.ToInt16(head);
        if (t == 0x10 || t == 0x11)
        {
            var lenBuf = new byte[4];
            await stream.ReadExactlyAsync(lenBuf);
            var body = new byte[BitConverter.ToInt32(lenBuf)];
            await stream.ReadExactlyAsync(body);
            Console.WriteLine(t == 0x11 ? $"[push 호가] {DescribeBook(body)}" : $"[push 체결] {DescribeTrade(body)}");
            continue;
        }
        var rest = new byte[2];
        await stream.ReadExactlyAsync(rest);
        code = BitConverter.ToInt32([head[0], head[1], rest[0], rest[1]]);
        Console.WriteLine($"response: {code}");
        drain.CancelAfter(500);
    }
    return code == 200 ? 0 : 2;
}

// ticker(4) + side(2) + price(8) + qty(4)
static string DescribeTrade(byte[] b) =>
    $"{Encoding.UTF8.GetString(b, 0, 4)} {(BitConverter.ToInt16(b, 4) == 1 ? "매수" : "매도")} " +
    $"{BitConverter.ToInt32(b, 14)}주 @{BitConverter.ToInt64(b, 6)}";

// ticker(4) + bidCount(4) + [price(8)+qty(4)]×N + askCount(4) + [price(8)+qty(4)]×M
static string DescribeBook(byte[] b)
{
    int o = 4;
    string Side()
    {
        int n = BitConverter.ToInt32(b, o); o += 4;
        var levels = new List<string>();
        for (int i = 0; i < n; i++, o += 12)
            levels.Add($"{BitConverter.ToInt64(b, o)}x{BitConverter.ToInt32(b, o + 8)}");
        return levels.Count == 0 ? "(없음)" : string.Join(", ", levels);
    }
    string ticker = Encoding.UTF8.GetString(b, 0, 4);
    string bids = Side();
    string asks = Side();
    return $"{ticker} 매수호가 [{bids}] 매도호가 [{asks}]";
}

// 대상 프로세스의 콘솔에 붙어 CTRL_C_EVENT를 보낸다. 대상은 자기 전용 콘솔에서 실행 중이어야 한다
// (같은 콘솔을 쓰는 다른 프로세스도 신호를 받기 때문).
static int SendCtrlC(uint pid)
{
    Native.FreeConsole();
    if (!Native.AttachConsole(pid)) return 10;
    Native.SetConsoleCtrlHandler(IntPtr.Zero, true); // 자신은 무시
    bool ok = Native.GenerateConsoleCtrlEvent(0 /* CTRL_C_EVENT */, 0);
    Thread.Sleep(500);
    Native.FreeConsole();
    return ok ? 0 : 11;
}

static async Task<int> RunSql(string query)
{
    await using var conn = new NpgsqlConnection(ConnStr);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand(query, conn);
    await using var reader = await cmd.ExecuteReaderAsync();
    do
    {
        if (reader.FieldCount == 0)
        {
            Console.WriteLine($"({reader.RecordsAffected} rows affected)");
            continue;
        }
        var cols = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Console.WriteLine(string.Join(" | ", cols));
        int n = 0;
        while (await reader.ReadAsync())
        {
            var vals = Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i)));
            Console.WriteLine(string.Join(" | ", vals));
            n++;
        }
        Console.WriteLine($"({n} rows)");
    } while (await reader.NextResultAsync());
    return 0;
}

static class Native
{
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool AttachConsole(uint pid);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool FreeConsole();
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool SetConsoleCtrlHandler(IntPtr handler, bool add);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool GenerateConsoleCtrlEvent(uint ctrlEvent, uint processGroupId);
}
