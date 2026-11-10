using EquityBackend.GameLogic.Service;
using Npgsql;

namespace EquityBackend.GameLogic;

public class DbManager
{
    private readonly string _connectionString = "Host=localhost;Username=postgres;Password=1020;Database=Equity";
    
    public OrderRepository _orderRepo;
    public UserRepository _userRepo;
    public InventoryRepository _inventoryRepo;
    public AssetsRepository _assetsRepo;
    public TradeRepository _tradeRepo;
    
    private NpgsqlConnection _connection;
    private NpgsqlTransaction _transaction;

    // 모든 DB 접근은 DbManager를 거치므로 여기서 한 번 설정한다.
    // snake_case 컬럼(base_price, tick_size, is_fractional 등)을 PascalCase 속성에 매핑.
    // 이름이 다른 컬럼(executed_qty → ExecutedQuantity, created_at → CreateAt)은 쿼리에서 별칭을 쓴다.
    static DbManager()
    {
        Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
    }

    public async Task DbConnection()
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync();
        _transaction = await _connection.BeginTransactionAsync();
        try
        {
           _orderRepo = new OrderRepository(_connectionString, _connection, _transaction);
           _userRepo = new UserRepository(_connectionString, _connection, _transaction);
           _inventoryRepo = new InventoryRepository(_connectionString, _connection, _transaction);
           _assetsRepo = new AssetsRepository(_connectionString, _connection, _transaction);
           _tradeRepo = new TradeRepository(_connectionString, _connection, _transaction);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    
    public async Task CommitAsync()
    {
        await _transaction.CommitAsync();
    }

    public async Task RollbackAsync()
    {
        await _transaction.RollbackAsync();
    }

    public async Task DisposeAsync()
    {
        await _connection.CloseAsync();
        await _transaction.DisposeAsync();
    }



}