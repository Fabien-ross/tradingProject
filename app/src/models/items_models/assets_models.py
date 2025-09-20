"""
version: dev_1.0.0

"""
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
from datetime import datetime
from typing import List
    
    
@dataclass
class BaseAsset:
    symbol: str
    type_id: str
    market_ids: Optional[List[str]] = None # Args to put in cross-table structures
    name: Optional[str] = None
    asset_id: str = "default_id"
    main_market_id: Optional[str] = None
    status: int = 0
    website: Optional[str] = None
    maj_date: Optional[datetime] = None
    
    
@dataclass
class BaseDerivativeAsset(BaseAsset):
    underlying_asset: Optional[str] = None
    leverage: Optional[Decimal] = None
    expiry_date: Optional[datetime] = None
    open_interest: Optional[int] = None
    premium: Optional[Decimal] = None
    margin_requirement: Optional[Decimal] = None

@dataclass
class BaseHybridAsset(BaseAsset):
    ticker: Optional[str] = None

@dataclass
class Crypto(BaseAsset):
    quote_asset: Optional[str] = None
    base_asset: Optional[str] = None

@dataclass
class Commodity(BaseAsset):
    commodity_type: Optional[str] = None
    unit: Optional[str] = None

@dataclass
class Forex(BaseAsset):
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    exchange_rate: Optional[Decimal] = None

@dataclass
class Equity(BaseAsset):
    pe_ratio: Optional[Decimal] = None
    dividend_yield: Optional[Decimal] = None

@dataclass
class Option(BaseDerivativeAsset):
    option_type: Optional[str] = None
    strike_price: Optional[Decimal] = None
    implied_volatility: Optional[Decimal] = None

@dataclass
class Future(BaseDerivativeAsset):
    contract_symbol: Optional[str] = None
    settlement_type: Optional[str] = None

@dataclass
class CFD(BaseDerivativeAsset):
    pass

@dataclass
class ETF(BaseHybridAsset):
    nav: Optional[Decimal] = None
    expense_ratio: Optional[Decimal] = None
    holdings_count: Optional[int] = None
    tracking_index: Optional[str] = None

@dataclass
class Bond(BaseHybridAsset):
    issuer: Optional[str] = None
    maturity_date: Optional[datetime] = None
    coupon_rate: Optional[Decimal] = None
    face_value: Optional[Decimal] = None
    current_yield: Optional[Decimal] = None
    rating: Optional[str] = None

