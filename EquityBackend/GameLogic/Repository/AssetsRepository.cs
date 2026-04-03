using System.Data;
using Dapper;

namespace EquityBackend.GameLogic;

public enum AssetType { STOCK, CRYPTO, COMMODITY, BOND}

public class Asset
{
    public string Ticker { get; set; }
    public string Name { get; set; }
    public AssetType AssetType { get; set; }
    public long BasePrice { get; set; }
    public int TickSize { get; set; }
    public bool IsFractional { get; set; }
    public long TotalSupply { get; set; }
    public bool IsActive { get; set; }
}

public class AssetRelation
{
    public string SourceAsset { get; set; }
    public string TargetAsset { get; set; }
    public float PositiveWeight { get; set; }
    public float NegativeWeight { get; set; }
}

public class AssetsRepository
{
    
    private readonly string _connectionString;
    private readonly IDbConnection _db;
    private readonly IDbTransaction _trx;
    

    public AssetsRepository(string connectionString, IDbConnection db, IDbTransaction trx)
    {
        _connectionString = connectionString;
        _db = db;
        _trx = trx;
    }

    public async Task<List<AssetRelation>> GetRelationAsync(string sourceAsset, string targetAsset)
    {
        string sql = @"
SELECT * FROM Assets WHERE (source_asset = @source_asset) AND (target_asset = @target_asset);";
        return (List<AssetRelation>)await _db.QueryAsync<AssetRelation>(sql,
            new { sourceAsset = sourceAsset, targetAsset = targetAsset }, transaction: _trx);
    }

    public async Task<Asset> GetAssetAsync(string ticker)
    {
        string sql = @"
SELECT * FROM Assets WHERE ticker = @ticker;";
        
        return await _db.QueryFirstOrDefaultAsync<Asset>(sql, new { ticker = ticker }, transaction: _trx);
    }

    public async Task<IEnumerable<Asset>> GetAssetsAsync()
    {
        string sql = @"SELECT * FROM Assets;";
        
        return await _db.QueryAsync<Asset>(sql: sql, _trx);
    }
}