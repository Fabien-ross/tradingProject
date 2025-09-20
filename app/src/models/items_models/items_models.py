"""
version: dev_1.0.0

"""
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
from datetime import datetime
from typing import List

@dataclass
class MarketInfo:
    market_id: str
    name: str
    website: str
    type_ids: List[str] # Arg to put in cross-table structures
    asset_number: int = 0
    status: int = 0
    maj_date: Optional[datetime] = None


@dataclass
class AssetType:
    name : str
    referent_markets : List[str]
    type_id : str = "default_id"
    maj_date: Optional[datetime] = None