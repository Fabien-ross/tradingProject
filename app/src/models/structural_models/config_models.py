"""

"""
from dataclasses import dataclass
from typing import Optional, Type
from decimal import Decimal
from pydantic import BaseModel, RootModel, ConfigDict
from typing import Dict, List, Any, Generic, TypeVar
import pandas as pd
from copy import deepcopy

from src.models.items_models.assets_models import BaseAsset
from src.models.items_models.items_models import MarketInfo
from src.models.lhrd_models.standard_models import TimeFrameContentMetaData

T = TypeVar('T')

@dataclass
class SchemaVersion:
    version: int
    applied_at: Optional[Decimal] = None

@dataclass
class RegisteredAssetType:
    referent_markets: List[str]
    cls: Type[BaseAsset]

class KlineData(BaseModel):
    klines : Optional[pd.DataFrame] = None
    tfc_metadata : Optional[TimeFrameContentMetaData] = None
    model_config: ConfigDict = ConfigDict(arbitrary_types_allowed=True)

class KlineConfig(BaseModel):
    asset: BaseAsset
    kline_data: Dict[str, KlineData]

class FullConfig(RootModel[Dict[str, Dict[str, List[T]]]]):
    """
    Must be like:

    {
        asset_type_id_1 : 
            {
                market_id_1 : [BaseAsset_1, BaseAsset_2, etc],
                market_id_2 : etc
            },
        asset_type_id_2 : etc
    }
    
    """
    def iter_config(self):
        for asset_type, markets in self.root.items():
            for market in markets:
                yield asset_type, market
    
    def add_item(
        self,
        asset_type_id: str,
        market_id: str,
        item: T
    ):
        if asset_type_id not in self.root:
            print(f"{asset_type_id} not allowed.")
            return 

        if market_id not in self.root[asset_type_id]:
            print(f"{market_id} not allowed.")
            return

        self.root[asset_type_id][market_id].append(item)
    
    def update(
        self,
        updt_dict : Dict[str, Dict[str, List[T]]]
    ):
        self.root = updt_dict

    def invert_key_order(self):
        """Invert asset_types & markets."""
        keys_1 = {}
        for it_2, it_1 in self.root.items():
            for key_1, val_1 in it_1.items():
                keys_1.setdefault(key_1, {})[it_2] = val_1

        cls = type(self)
        return cls(root=keys_1)
    
    def merge_configs(self, snd_config: 'FullConfig'):
        """
        Merge snd_config in self, without duplicates.
        """
        if type(self) is not type(snd_config):
            raise TypeError(
                f"Cannot merge {type(snd_config).__name__} into {type(self).__name__}"
            )

        for asset_type, markets in snd_config.root.items():
            if asset_type not in self.root:
                self.root[asset_type] = {m: list(items) for m, items in markets.items()}
                continue

            for market, items in markets.items():
                if market not in self.root[asset_type]:
                    self.root[asset_type][market] = list(items)
                else:
                    existing_items = self.root[asset_type][market]
                    for item in items:
                        if item not in existing_items:
                            existing_items.append(item)

    def to(self, cls):
        new_obj = cls.__new__(cls)
        new_obj.__dict__ = deepcopy(self.__dict__)
        return new_obj


class FullAssetConfig(FullConfig[BaseAsset]):

    def print_el(self) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
        """Retourne une représentation dict bien formée."""
        out = {}
        for asset_type, markets in self.root.items():
            out[asset_type] = {}
            for market, ass_list in markets.items():
                out[asset_type][market] = [
                    {"symbol": ass.symbol, "status": ass.status}
                    for ass in ass_list
                ]
        return out

    def make_kline_config(self) -> 'FullKlineConfig':
        kline_root: Dict[str, Dict[str, List[KlineConfig]]] = {}

        for asset_type, markets in self.root.items():
            kline_root[asset_type] = {}
            for market, assets in markets.items():
                kline_list: List[KlineConfig] = []
                for asset in assets:
                    kline_cfg = KlineConfig(
                        asset=asset,
                        kline_data={}
                    )
                    kline_list.append(kline_cfg)
                kline_root[asset_type][market] = kline_list

        return FullKlineConfig(root=kline_root)


class FullKlineConfig(FullConfig[KlineConfig]):

    def make_asset_config(self) -> 'FullAssetConfig':
        asset_root: Dict[str, Dict[str, List[BaseAsset]]] = {}

        for asset_type, markets in self.root.items():
            asset_root[asset_type] = {}
            for market, kline_list in markets.items():
                asset_list: List[BaseAsset] = []
                for kline_cfg in kline_list:
                    asset_list.append(kline_cfg.asset)
                asset_root[asset_type][market] = asset_list

        return FullAssetConfig(root=asset_root)
    
    def print_el(self) :
        pass


class FullAnyConfig(FullConfig[Any]):
    pass