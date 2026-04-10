CREATE TABLE IF NOT EXISTS asset_relations (
    source_asset VARCHAR(4) NOT NULL REFERENCES assets(ticker),
    target_asset VARCHAR(4) NOT NULL REFERENCES assets(ticker),
    positive_weight FLOAT NOT NULL DEFAULT 0,
    negative_weight FLOAT NOT NULL DEFAULT 0,
    PRIMARY KEY (source_asset, target_asset)
);

-- 샘플 데이터: 원유(OIL1)가 오르면 항공주(AIR1)는 하락, 정유주(REF1)는 상승
-- INSERT INTO asset_relations VALUES ('OIL1', 'AIR1', -0.3, 0.1);
-- INSERT INTO asset_relations VALUES ('OIL1', 'REF1',  0.4, -0.2);
