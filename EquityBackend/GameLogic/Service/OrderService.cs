using System.Data.Common;

namespace EquityBackend.GameLogic.Service;

public class AssetNotFoundException : Exception { }
public class InsufficientFundsException : Exception { }
public class TickSizeException : Exception { }

public class OrderService
{
    private readonly string _connectionString = "Host=localhost;Username=postgres;Password=1020;Database=Equity";
    private DbManager _dbManager;
    public OrderService(DbManager manager)
    {
        _dbManager = manager;
    }
    
    // 주문 시 묶는 금액과 체결 정산 금액이 반드시 같은 규칙을 쓰도록 한 곳에 둔다
    // (IsFractional 자산은 price * quantity 를 10,000,000 으로 나눈다 — 정수 나눗셈)
    public const long FractionalDivisor = 10_000_000;

    public static long CalculateAmount(long price, long quantity, bool isFractional)
    {
        long amount = price * quantity;
        if (isFractional)
            amount /= FractionalDivisor;
        return amount;
    }

    public async Task<Order> CreateBuyOrder(long userId, string ticker, int amount, int price)
    {
        Order newOrder = new Order 
        {
            UserId = userId, 
            Ticker = ticker,
            Side = OrderSide.Buy, // 매수 주문!
            Price = price,
            Quantity = amount,
            Status = OrderStatus.PENDING, // 아직 체결 안 됨(대기 중)
            CreateAt = DateTime.UtcNow
        };

        await _dbManager.DbConnection();
        try
        {
            var asset = await _dbManager._assetsRepo.GetAssetAsync(ticker);

            if (asset == null)
                throw new AssetNotFoundException();

            long priceAmount = CalculateAmount(price, amount, asset.IsFractional);


            if (await _dbManager._userRepo.GetUserBalance(userId) < priceAmount)
                throw new InsufficientFundsException();

            if (price %
                asset.TickSize != 0)
                throw new TickSizeException();
            await _dbManager._userRepo.UpdateLockedBalance(userId, priceAmount);
            await _dbManager._userRepo.UpdateBalance(userId, -priceAmount);
            newOrder.OrderId = await _dbManager._orderRepo.InsertOrderAsync(newOrder);

            await _dbManager.CommitAsync();
            return newOrder;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            await _dbManager.RollbackAsync();
            throw;
        }
        finally
        {
            await _dbManager.DisposeAsync();
        }
    }

    public async Task<Order> CreateSellOrder(long userId, string ticker, int amount, int price)
    {
        Order newOrder = new Order()
        {
            UserId = userId,
            Ticker = ticker,
            Side = OrderSide.Sell,
            Price = price,
            Quantity = amount,
            Status = OrderStatus.PENDING,
            CreateAt = DateTime.UtcNow
        };

        await _dbManager.DbConnection();
        try
        {
            var asset = await _dbManager._assetsRepo.GetAssetAsync(ticker);
            if (asset == null)
                throw new AssetNotFoundException();
                
            if (price %
                asset.TickSize != 0)
                throw new TickSizeException();
                
            if (await _dbManager._inventoryRepo.GetInventoryQuantity(userId, ticker) < amount)
            {
                throw new InsufficientFundsException();
            }
            
            await _dbManager._inventoryRepo.UpdateLockedInventory(userId, ticker, amount);
            await _dbManager._inventoryRepo.UpdateInventory(userId, ticker, -amount);
            newOrder.OrderId = await _dbManager._orderRepo.InsertOrderAsync(newOrder);

            await _dbManager.CommitAsync();
            return newOrder;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            await _dbManager.RollbackAsync();
            throw;
        }
        finally
        {
            await _dbManager.DisposeAsync();
        }
    }
}