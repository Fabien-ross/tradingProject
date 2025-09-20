from typing import Dict, Type
from src.models.items_models.base_market import BaseMarket
from src.models.structural_models.config_models import RegisteredAssetType, FullConfig

ASSET_TYPE_RGSTR :  Dict[str, RegisteredAssetType] = {}
MARKET_RGSTR : Dict[str, Type[BaseMarket]] = {}
BASE_ASSET_RTRV_CONFIG : FullConfig = FullConfig({})