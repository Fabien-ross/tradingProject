import pandas as pd
import asyncio
import signal

from typing import Optional, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta, timezone

from src.core.logging.loggers import logger_database, logger_structure
from src.core.utils.helpers.display_helper import spinner
from src.core.exceptions.exceptions import *
from src.core.data.default import (
    ASSET_TYPE_RGSTR, 
    BASE_ASSET_RTRV_CONFIG, 
    MARKET_RGSTR, 
)

from src.databases.database import Database
from src.databases.migration.database_migration import DatabaseMigration

from src.execution.lhdr_executor import LhdrExecutor
from src.execution.structural_executor import StructuralExecutor
from src.execution.display_executor import DisplayExecutor

from src.models.items_models.items_models import MarketInfo
from src.models.structural_models.config_models import FullAssetConfig

class ProductionOrchestrator:
    

    def __init__(self):
        self.struct_exec = StructuralExecutor()
        self.display_exec = DisplayExecutor()
        self.lhdr_exec = LhdrExecutor()
        self.db = Database()
        self.db_migr = DatabaseMigration()
        self.base_assets_config: FullAssetConfig
        self.laac_delta : timedelta = timedelta(days=1)

        # Specify LiveData parameters (200 klines, dates etc)


    async def DEV_table_rase(self):
        try:
            self.db_migr.reset_alembic_db()
            self.db.drop_all_tables()
        except Exception as e:
            logger_database.exception({str(e)})


    async def launch_db(self) -> bool:
        try:
            self.db_migr.db_structure_update()
            return True 
        except Exception as e:
            logger_database.exception(str(e))
            return False 


    async def check_and_update_markets(self) -> Optional[bool]:

        try:
            core_markets, core_asset_types = self.struct_exec.get_base_config()
            MARKET_RGSTR.update(self.struct_exec.update_market_registry(core_markets))
            active_markets : List[MarketInfo] = await self.lhdr_exec.markets_api_check(markets=core_markets)
            self.struct_exec.update_asset_type_registry(asset_types=core_asset_types)
            await self.db.update_markets_and_asset_types(
                markets=active_markets,
                supported_types=core_asset_types)
            logger_database.info("Markets and asset types updated.")
            
            self.struct_exec.update_base_asset_retrieving_config()
            
            return True
        except MarketAvailabilityError:
            logger_structure.exception("[MARKET ERROR] No market API available.")
            return
        except NoMarketSupported:
            logger_database.exception("[MARKET ERROR] No market in 'Markets' table and no market supported.")
        except Exception as e:
            logger_structure.rooted_exception(f"Details : {e}.")
            return


    async def update_assets_tables(
        self,
        ass_nb_limit:int = 10
    ) -> Optional[bool]:

        try:
            assets_config = await self.lhdr_exec.get_markets_assets_config()
            no_laac_assets_config = BASE_ASSET_RTRV_CONFIG.to(FullAssetConfig)
            df_db_assets_config = self.db.read_active_mrk_assets_to_df()

            if not df_db_assets_config.empty:
                db_assets_config = await self.struct_exec.df_to_asset_config(df=df_db_assets_config)
                df_db_assets_config["maj_date"] = pd.to_datetime(df_db_assets_config["maj_date"], utc=True)
                make_strong_laac = (df_db_assets_config["maj_date"].min() < (datetime.now(timezone.utc) - self.laac_delta))

                no_laac_assets_config = await self.struct_exec.spot_laac_assets(
                    assets_config,
                    no_laac_assets_config, 
                    db_assets_config,
                    make_strong_laac
                )

        except MarketNameError as e:
            logger_structure.rooted_exception(f"Details : {str(e)}")
            return
        except Exception as e:
            logger_structure.rooted_exception(f"Details : {str(e)}")
            return
        
        try:
            laac_processed_fklnc = await self.lhdr_exec.laac_process(assets_config=assets_config.make_kline_config())
            assets_config = laac_processed_fklnc.make_asset_config()
            if no_laac_assets_config:
                assets_config.merge_configs(no_laac_assets_config)
            for at_id in assets_config.root.keys():
                await self.db.update_assets(
                    asset_type_id=at_id,
                    assets_by_markets=assets_config.root[at_id],
                    asset_number_limit=ass_nb_limit
                    )
            return True
            
        except Exception as e:
            logger_structure.rooted_exception(f"Details : {str(e)}")


    async def historical_catchup(
        self,
        data_table_name: str,
        deletion_only: bool = False,
        kline_count: int=200,
    ) -> Optional[bool]:
        
        df_db_assets = self.db.read_table_to_df(specified_table="Assets")
        catchup_live_data_state = self.db.get_db_data_state(table_name=data_table_name)

        deprecated_asset_ids, klines_rtrv_assets_config, time_segments = self.struct_exec.catchup_config(
            count=kline_count,
            df_assets=df_db_assets,
            data_state=catchup_live_data_state
        )

        if not deletion_only:

            live_data_df: pd.DataFrame = await self.lhdr_exec.lhdr_klines(
                kln_config=klines_rtrv_assets_config,
                ponctual=False
            )

            self.db.delete_content_by_asset_id(
                table_name=data_table_name,
                asset_ids=deprecated_asset_ids
            )
            self.db.write_df(
                df=live_data_df,
                table_name=data_table_name
            )

        self.db.delete_deprecated_data(
            time_segs=time_segments,
            table_name=data_table_name
        )
        
        return True


    async def ponctual(self, time_frames: List[str]):

        await asyncio.sleep(3)

        df_db_live_data = self.db.read_table_to_df(specified_table="LiveData")
        df_db_assets = self.db.read_table_to_df(specified_table="Assets")

        klines_rtrv_assets_config = self.struct_exec.ponctual_config(
            df_data=df_db_live_data,
            df_assets = df_db_assets,
            tfs=time_frames
        )

        new_klines: pd.DataFrame = await self.lhdr_exec.lhdr_klines(kln_config=klines_rtrv_assets_config)

        self.db.write_df(
            df=new_klines,
            table_name="LiveData"
        )

        if '1h' in time_frames:
            await asyncio.sleep(20)
            await self.update_assets_tables()
        
        if '1d' in time_frames:
            await asyncio.sleep(20)
            await self.check_and_update_markets()
            await asyncio.sleep(20)
            await self.historical_catchup(data_table_name="LiveData", deletion_only=True)


    async def run_ponctuals(self):

        # NOT IMPLEMENTED : add smth that verifies that every asset is up to date in LiveData

        async def run_job():
            now = datetime.now(timezone.utc).replace(microsecond=0)
            tfs = []

            if now.minute % 5 == 0:
                tfs.append("5m")
            if now.minute % 15 == 0:
                tfs.append("15m")
            if now.minute == 0:
                tfs.append("1h")
            if now.minute == 0 and now.hour % 4 == 0:
                tfs.append("4h")
            if now.hour == 0 and now.minute == 0:
                tfs.append("1d")

            if tfs:
                await self.ponctual(time_frames=tfs)
                logger_structure.info(f"Successfully ran job with time frames {tfs}.")

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop_event.set)

        spinner_task = asyncio.create_task(spinner(stop_event))
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(run_job, "cron", second=0, misfire_grace_time=30)
        scheduler.start()

        await stop_event.wait()
        scheduler.shutdown()
        spinner_task.cancel()
        logger_structure.info("Ponctuals stopped.")

