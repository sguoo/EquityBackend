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