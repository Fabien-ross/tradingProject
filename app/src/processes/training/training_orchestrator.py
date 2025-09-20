import pandas as pd
from typing import Optional, List
from datetime import datetime

from src.core.logging.loggers import logger_spo, logger_database, logger_structure
from src.core.exceptions.exceptions import *

from src.databases.database import Database

from src.execution.lhdr_executor import LhdrExecutor
from src.execution.structural_executor import StructuralExecutor
from src.execution.display_executor import DisplayExecutor


class TrainingOrchestrator:

    def __init__(self, asset_ids : List[str]):

        self.asset_ids: List[str] = asset_ids
        self.struct_exec = StructuralExecutor()
        self.lhdr_exec = LhdrExecutor()
        self.display_exec = DisplayExecutor()
        self.db = Database()

    async def get_historical(
        self,
        data_table_name: str = "TrainingData",
        kline_count:int=500,
        latest_time : Optional[datetime] = None,
        from_scratch: bool = False
    ) -> Optional[bool]:
        
        df_db_assets = self.db.read_table_to_df(specified_table="Assets")
        df_db_assets = df_db_assets[df_db_assets["asset_id"].isin(self.asset_ids)]

        if df_db_assets.empty:
            logger_structure.exception(f"No such assets as those in provided list : {self.asset_ids}.")
            return False

        wanted_assets = list(df_db_assets["asset_id"])
        
        if from_scratch:
            self.db.delete_content_by_asset_id(
                table_name=data_table_name,
                asset_ids=wanted_assets
            )

        training_data_state = self.db.get_db_data_state(table_name=data_table_name)
        

        _, klines_rtrv_assets_config, _ = self.struct_exec.catchup_config(
            count=kline_count,
            df_assets=df_db_assets,
            data_state=training_data_state,
            latest_time=latest_time
        )

        training_data_df: pd.DataFrame = await self.lhdr_exec.lhdr_klines(
            kln_config=klines_rtrv_assets_config,
            ponctual=False
        )
       
        self.db.write_df(
            df=training_data_df,
            table_name=data_table_name
        )

        return True


    async def display(self):

        df_db_training_data = self.db.read_table_to_df(specified_table="TrainingData")
        df_db_training_data = df_db_training_data[df_db_training_data["asset_id"].isin(self.asset_ids)]
        self.display_exec.plot_klines_and_indicators(df_data = df_db_training_data)
