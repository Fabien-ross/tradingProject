"""

"""
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal

@dataclass
class Position:
    position_id: str
    asset_id: str
    oldest_date: Decimal
    latest_date: Optional[Decimal]
    status: int
    log_message: Optional[str]
    starting_amount: Optional[Decimal]
    usd_amount: Optional[Decimal]
    max_amount: Optional[Decimal]
    score: Optional[Decimal]
    horizon: Optional[str]


@dataclass
class Transaction:
    """
    
    """
    general_data: Optional[str] = None
    action: Optional[str] = None  # buy (1) or sell (0)
    usd_amount: Optional[Decimal] = None
    amount: Optional[Decimal] = None
    expected_fee_usd: Optional[Decimal] = None
    maximum_fee_usd: Optional[Decimal] = None
    score: Optional[Decimal] = None