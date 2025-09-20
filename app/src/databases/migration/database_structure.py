from sqlalchemy import (
    MetaData, Table, Column, String, Text, Integer, DECIMAL, Numeric,
    TIMESTAMP, ForeignKey, UniqueConstraint
)

structure_metadata = MetaData()


SchemaVersions = Table("SchemaVersions", structure_metadata,
    Column("version", Text),
    Column("applied_at", TIMESTAMP, server_default="now()")
)

MarketAssetTypes = Table("MarketAssetTypes", structure_metadata,
    Column("market_id", String, ForeignKey("Markets.market_id"), primary_key=True),
    Column("type_id", String, ForeignKey("AssetTypes.type_id"), primary_key=True)
)

AssetMarkets = Table("AssetMarkets", structure_metadata,
    Column("asset_id", String, ForeignKey("Assets.asset_id"), primary_key=True),
    Column("market_id", String, ForeignKey("Markets.market_id"), primary_key=True)
)

AssetTypes = Table("AssetTypes", structure_metadata,
    Column("type_id", String, primary_key=True),  
    Column("name", String, nullable=False),
    Column("maj_date", TIMESTAMP)
)

Markets = Table("Markets", structure_metadata,
    Column("market_id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("asset_number", Integer),
    Column("status", Integer),
    Column("website", Text),
    Column("maj_date", TIMESTAMP)
)

Assets = Table("Assets", structure_metadata,
    Column("asset_id", String, primary_key=True),
    Column("symbol", String(20), nullable=False),
    Column("name", String),
    Column("main_market_id", String, ForeignKey("Markets.market_id")),
    Column("type_id", String, ForeignKey("AssetTypes.type_id")),
    Column("status", Integer),
    Column("website", Text),
    Column("maj_date", TIMESTAMP)
)

Cryptos = Table("Cryptos", structure_metadata,
    Column("asset_id", String, ForeignKey("Assets.asset_id"), primary_key=True),
    Column("quote_asset", String(20)),
    Column("base_asset", String(20))
)

Cryptos = Table("Futures", structure_metadata,
    Column("asset_id", String, ForeignKey("Assets.asset_id"), primary_key=True),
    Column("contract_symbol", String(20)),
    Column("settlement_type", String(20))
)

Positions = Table("Positions", structure_metadata,
    Column("position_id", String, primary_key=True),
    Column("asset_id", String, ForeignKey("Assets.asset_id"), nullable=False),
    Column("oldest_date", TIMESTAMP, nullable=False),
    Column("latest_date", TIMESTAMP),
    Column("status", Integer, nullable=False),
    Column("log_message", Text),
    Column("starting_amount", Numeric(20, 8)),
    Column("amount", Numeric(20, 8)),
    Column("usd_value", Numeric(20, 8)),
    Column("max_amount", Numeric(20, 8)),
    Column("horizon", Text),
)

def make_indicator_table(name):
    return Table(name, structure_metadata,
        Column("asset_id", String, ForeignKey("Assets.asset_id")),
        Column("open_time", TIMESTAMP),
        Column("time_frame", String),
        Column("open", DECIMAL),
        Column("high", DECIMAL),
        Column("low", DECIMAL),
        Column("close", DECIMAL),
        Column("volume", DECIMAL),
        Column("rsi", DECIMAL),
        Column("stoch_rsi", DECIMAL),
        Column("macd", DECIMAL),
        Column("ema_short", DECIMAL),
        Column("ema_mid", DECIMAL),
        Column("ema_long", DECIMAL),
        Column("boll_low2", DECIMAL),
        Column("boll_low1", DECIMAL),
        Column("boll_mid", DECIMAL),
        Column("boll_up1", DECIMAL),
        Column("boll_up2", DECIMAL),
        Column("msd", DECIMAL),
        Column("simple_return", DECIMAL),
        Column("log_return", DECIMAL),
        Column("obv", DECIMAL),
        Column("vwap", DECIMAL),
        Column("volatility", DECIMAL),
        Column("score", DECIMAL),
        UniqueConstraint("asset_id", "time_frame", "open_time", name=f"uq_{name.lower()}_asset_time")
    )

LiveData = make_indicator_table("LiveData")
TrainingData = make_indicator_table("TrainingData")
