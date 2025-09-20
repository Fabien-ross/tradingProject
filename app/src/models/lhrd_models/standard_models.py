from typing import Optional, Dict, List
from datetime import datetime, timezone

from src.core.utils.dates.date_format import get_unix_time_s, interval_map
from src.core.logging.loggers import logger_structure

class TimeFrameContentMetaData:
    """Delta of UNIX timestamps (always in seconds) that define symbol data extent."""

    def __init__(self, time_frame : str, latest_time: datetime, oldest_time: datetime):
        self.time_frame = time_frame
        self.latest_time = latest_time.astimezone(timezone.utc)
        self.oldest_time = oldest_time.astimezone(timezone.utc)


    def time_segment_to_dict(self) -> Dict[str, datetime]:
        return {
            "latest_time": self.latest_time,
            "oldest_time": self.oldest_time
        }


class ContentDataState:
    """Symbols data & timeframes organisation."""

    def __init__(self):
        self.data: Dict[str, Dict[str, TimeFrameContentMetaData]] = {}
        self.time_frames = list(interval_map.keys())
    

    def is_empty(self):
        return self.data == {}


    def add_asset(self, asset_id: str):
        if asset_id not in self.data:
            self.data[asset_id] = {}
            

    def delete_asset(self, asset_id: str):
        self.data.pop(asset_id,None)


    def delete_asset_tfc(self, asset_id: str, tf: str):
        self.data.get(asset_id,{}).pop(tf,None)


    def update_timeframe_data_given_limit(
        self,
        asset_ids : str|List[str],
        latest_time: datetime,
        limit : int
    ):
        for time_frame in self.time_frames:
            floored_latest_time, oldest_time = get_unix_time_s(
                count=limit, 
                latest_time=latest_time, 
                time_frame=time_frame
                )
            
            tfc_metadata = TimeFrameContentMetaData(
                time_frame=time_frame,
                latest_time=floored_latest_time,
                oldest_time=oldest_time
            )
            for asset in asset_ids:
                self.update_metadata_of_asset(
                    asset_id=asset,
                    tfc_metadata=tfc_metadata,
                    time_frame=time_frame
                )


    def update_timeframe_data(
        self, 
        asset_ids: str|List[str],
        latest_time: datetime,
        oldest_time: datetime,
        time_frame: str,
    ):
        
        if time_frame and time_frame not in self.time_frames:
            raise KeyError(f"Invalid time_frame : {time_frame}.")
        
        if isinstance(asset_ids, str):
            asset_ids = [asset_ids]

        for asset in asset_ids:
            self.add_asset(asset_id=asset)
            self.data[asset][time_frame] = TimeFrameContentMetaData(
                time_frame=time_frame,
                latest_time=latest_time,
                oldest_time=oldest_time
            )


    def update_metadata_of_asset(
        self,
        asset_id : str,
        time_frame : str,
        tfc_metadata : TimeFrameContentMetaData
    ):
        self.add_asset(asset_id=asset_id)
        self.data[asset_id][time_frame] = tfc_metadata
    
    
    def compare_with_table_state(
        self, 
        table_content : 'ContentDataState'
    )->'ContentDataState':
        
        to_retrieve = ContentDataState()
        for asset_id in self.data.keys(): 
            if asset_id not in table_content.data.keys():
                to_retrieve.data[asset_id] = self.data[asset_id]
            else:
                for time_frame in self.data[asset_id].keys(): 
                    imposed_metadata_unix : TimeFrameContentMetaData = self.data[asset_id][time_frame]
                    tfc_metadata : Optional[TimeFrameContentMetaData] = None
                    if time_frame not in table_content.data[asset_id].keys():
                        tfc_metadata = imposed_metadata_unix
                        
                    else:
                        table_metadata_unix = table_content.data[asset_id][time_frame]
                        logger_structure.debug(f"Imposed search {asset_id} - {time_frame.ljust(3)} : {imposed_metadata_unix.time_segment_to_dict()}")
                        logger_structure.debug(f"Table contains {asset_id} - {time_frame.ljust(3)} : {table_metadata_unix.time_segment_to_dict()}")
                        if table_metadata_unix.time_segment_to_dict() != imposed_metadata_unix.time_segment_to_dict():
                            tfc_metadata = imposed_metadata_unix

                    if tfc_metadata is not None:
                        to_retrieve.update_metadata_of_asset(
                            asset_id=asset_id,
                            time_frame=time_frame,
                            tfc_metadata=self.data[asset_id][time_frame]
                            )
        return to_retrieve
    

class MarketAssetTypes:
    pass