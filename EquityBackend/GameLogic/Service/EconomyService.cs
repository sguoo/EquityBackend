namespace EquityBackend.GameLogic.Service;

public class EconomyService
{
    DbManager _dbManager;
    public EconomyService(DbManager dbManager)
    {
        _dbManager = dbManager;
    }
    
    public async Task CentralBankLoop()
    {
        while (true)
        {
            try
            {
                await _dbManager.DbConnection();
                await _dbManager._userRepo.AddDailyInterestAsync((decimal)1.02);
                await _dbManager.CommitAsync();
            }
            catch (Exception ex)
            {
                await _dbManager.RollbackAsync();
                Console.WriteLine(ex.Message);
            }
            finally
            {
                await _dbManager.DisposeAsync();
            }
            await Task.Delay(TimeSpan.FromDays(1));
        }
    }
}