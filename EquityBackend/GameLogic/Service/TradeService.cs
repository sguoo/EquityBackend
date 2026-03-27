using Npgsql;

namespace EquityBackend.GameLogic.Service;

public class TradeService
{
    private readonly string _connectionString = "Host=localhost;Username=postgres;Password=1020;Database=Equity";

    
    OrderRepository _orderRepository;
    private readonly UserRepository _userRepo;
    private readonly InventoryRepository _inventoryRepo;
    DbManager _dbManager;
    public TradeService(OrderRepository orderRepository, UserRepository userRepository, InventoryRepository inventoryRepository)
    {
        _orderRepository = orderRepository;
        _userRepo = userRepository;
        _inventoryRepo = inventoryRepository;
    }

    public async Task DbTransaction(long sendUserId, long receiveUserId, string ticker, int moneyAmount, int quantityAmount)
    {
        await using (NpgsqlConnection conn = new(_connectionString))
        {
            await conn.OpenAsync();

            await using var tx = await conn.BeginTransactionAsync();
            try
            {
                await _userRepo.UpdateBalance(userId: sendUserId, amount: moneyAmount);
                await _inventoryRepo.UpdateLockedInventory(userId: sendUserId, ticker: ticker, amount: -quantityAmount);

                await _userRepo.UpdateLockedBalance(userId: receiveUserId, amount: -moneyAmount);
                await _inventoryRepo.UpdateInventory(userId: receiveUserId, ticker: ticker, amount: quantityAmount);

                await _dbManager.CommitAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                await _dbManager.RollbackAsync();
            }
        }
    }
}