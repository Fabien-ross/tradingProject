import pandas as pd
import sqlalchemy as sqlalch
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect, MetaData, func, select, Table, delete, update, Connection, and_, case, or_
from typing import List, Optional, Any, Tuple, Literal, Dict, cast
from decimal import Decimal
from datetime import datetime, timezone
from collections import Counter

from src.core.data.default import (
    ASSET_TYPE_RGSTR, 
    BASE_ASSET_RTRV_CONFIG, 
    MARKET_RGSTR, 
)

from src.core.exceptions.exceptions import *
from src.core.utils.helpers.io_helpers import ask_confirmation
from src.core.utils.config.secret_management import DATABASE_URL
from src.core.logging.loggers import logger_database

from src.databases.migration.database_structure import structure_metadata

from src.models.items_models.assets_models import BaseAsset, Crypto, Future
from src.models.items_models.items_models import AssetType
from src.models.items_models.items_models import MarketInfo
from src.models.lhrd_models.standard_models import ContentDataState, TimeFrameContentMetaData
from src.models.structural_models.config_models import KlineConfig


class Database:

    def __init__(self):
        self.db_version = 1
        self.engine = sqlalch.create_engine(DATABASE_URL)


    def check_table(
        self,
        table_name: str
    )-> Table:
        
        table: Optional[Table] = structure_metadata.tables.get(table_name)
        if table is None:
            raise InvalidTableNameError(table_name=table_name)
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names():
            raise TableNotFoundError(table_name=table_name)
        return table


    def get_db_data_state(
        self, 
        table_name: str = "TrainingData"
    ) -> ContentDataState:
        
        table_content_data_state = ContentDataState()
        
        if table_name.endswith("Data"):
            table: Table = self.check_table(table_name)
            stmt = select(
                table.c.asset_id,
                table.c.time_frame,
                func.max(table.c.open_time).label("latest_time"),
                func.min(table.c.open_time).label("oldest_time")
            ).group_by(
                table.c.asset_id,
                table.c.time_frame
            )
            with self.engine.connect() as conn:
                result = conn.execute(stmt).fetchall()

            for asset_id, time_frame, latest_time, oldest_time in result:
                try:
                    tfc_metadata = TimeFrameContentMetaData(
                        time_frame=time_frame,
                        latest_time=latest_time,
                        oldest_time=oldest_time)
                    
                    table_content_data_state.update_metadata_of_asset(
                        asset_id=asset_id,
                        tfc_metadata=tfc_metadata,
                        time_frame=time_frame
                    )
                except KeyError:
                    raise KeyError
            return table_content_data_state
        else:
            raise InvalidTableNameError(table_name=table_name)


    def clean_quantitative_indicators(
        self,
        unix_date_s : Decimal
    ):
        
        # Delete indicators previous to specified date
        # from the database.

        pass


    def delete_deprecated_data(
        self,
        time_segs:Dict[str,tuple[datetime,datetime]],
        table_name: str
    ):
        if not table_name.endswith("Data"):
            logger_database.warning(f"Table {table_name} doesn't have dated deprecated data.")
            return
        table: Table = self.check_table(table_name=table_name)
        row_nb = 0
        if table is not None:
            conditions = []
            for k, (_, old_time) in time_segs.items():
                conditions.append(
                    (table.c.time_frame == k) & (table.c.open_time < old_time)
                )

            if conditions:
                stmt = delete(table).where(or_(*conditions))
                with self.engine.begin() as conn:
                    row_nb = conn.execute(stmt).rowcount or 0
        
        if row_nb == 0:
            logger_database.info(f"No deprecated data to remove from table {table_name}.")
        else:
            logger_database.info(f"Deprecated data successfully removed from table {table_name} ({row_nb} rows).")


    def delete_table_full_content(
        self, 
        table_name: str
    ):
        if ask_confirmation(f"\nTable '{table_name}' will be deleted. Confirm"):
            table = structure_metadata.tables.get(table_name)
            if table is not None:
                with self.engine.connect() as conn:
                    conn.execute(table.delete())
                    conn.commit()
                    logger_database.info(f"Table {table_name} successfully erased.")
            else:
                raise TableNotFoundError(table_name=table_name)
        else:
            logger_database.info("Task 'delete_table_full_content' canceled.")


    def delete_content_by_asset_id(
        self, 
        table_name: str, 
        asset_ids: str|List[str], 
        time_frame: Optional[str] = None
    ):
        if isinstance(asset_ids,str):
            asset_ids = [asset_ids]
        if asset_ids == []:
            logger_database.info(f"No asset to remove from table {table_name}.")
            return
        table = structure_metadata.tables.get(table_name)
        row_nb = 0
        if table is not None:
            
            stmt = delete(table).where(table.c.asset_id.in_(asset_ids))
            
            if time_frame:
                stmt = stmt.where(table.c.time_frame == time_frame)

            with self.engine.begin() as conn:
                row_nb = conn.execute(stmt).rowcount or 0

        else:
            raise TableNotFoundError(table_name=table_name)
        
        if row_nb == 0:
            logger_database.debug(f"Nothing was removed from table {table_name}.")
            return
        
        logger_database.info(f"Symbol(s) {asset_ids} successfully removed from {table_name} ({row_nb} rows).")


    def drop_all_tables(self):
        if ask_confirmation("\nEvery table will be deleted (excluding alembic_version). Confirm"):
            meta_drop = MetaData()
            meta_drop.reflect(bind=self.engine)

            sorted_tables = [
                table for table in reversed(meta_drop.sorted_tables)
                if table.name != "alembic_version"
            ]

            for table in sorted_tables:
                logger_database.info(f"Dropping table: {table.name}")
                table.drop(bind=self.engine)

            logger_database.info("All tables (except 'alembic_version') have been successfully deleted.")
        else:
            logger_database.info("Task 'drop_all_tables' canceled.")


    def get_asset_id(
        self, 
        symbol: str,
        asset_type: str,
    ) -> str:
        
        table_name = "asset_type"
        table: Table = self.check_table(table_name=table_name)

        raise NotImplemented


    def get_tables(self) -> List[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema="public")


    def get_market_id(
        self, 
        market_name: str
    ) -> str:
        
        table_name = "Markets"
        table: Table = self.check_table(table_name)

        try:
            with self.engine.connect() as conn:
                stmt = select(table.c.market_id).where(table.c.name == market_name)
                result = conn.execute(stmt).fetchone()
                
                if result is None:
                    raise ValueError(f"Market name '{market_name}' not found in table '{table_name}'.")

                return result[0]
        except Exception as e:
            raise InvalidTableNameError(table_name=table_name) from e


    def get_live_assets(self, market_name:str) -> List[BaseAsset]:
        raise NotImplemented


    def read_data(
        self, 
        table_name: str,
        symbol: str,
        time_frame: str) -> pd.DataFrame:

        if table_name.endswith("Data"):
            klines_table = Table(table_name, structure_metadata, autoload_with=self.engine)
            stmt = select(klines_table).where(
                klines_table.c.asset_id == symbol,
                klines_table.c.time_frame == time_frame
            )

            df = pd.read_sql(stmt, self.engine)
        else:
            raise InvalidTableNameError(table_name=table_name)
        return df


    def read_table_to_df(
        self,
        specified_table: str|Table, 
    ) -> pd.DataFrame:

        if isinstance(specified_table, str):
            table: Table = self.check_table(specified_table)
        else:
            table = specified_table

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(select(table), conn)
        except Exception as e:
            raise InvalidTableNameError(table_name=specified_table) from e

        return df


    def read_active_mrk_assets_to_df(self):
        table_name = "Assets"
        table: Table = self.check_table(table_name)
        market_ids = list(MARKET_RGSTR.keys())
        try:
            with self.engine.connect() as conn:
                query = select(table).where(table.c.main_market_id.in_(market_ids))
                df = pd.read_sql(query, conn)
        except Exception as e:
            raise InvalidTableNameError(table_name=table_name) from e

        return df


    def read_named_columns_in_table(
        self,
        column_names: List[str],
        table: Table,
        table_name: str,
        check_for_duplicates: Optional[Literal["first_arg","all_args"]] = None
    ) -> Tuple[List[Tuple[Any, ...]], Optional[List[Tuple[Any, ...]]]]:

        for col in column_names:
            if col not in table.c:
                raise ValueError(f"Couldn't find '{col}' in table '{table_name}'.")

        selected_columns = [getattr(table.c, col) for col in column_names]
        stmt = select(*selected_columns)

        with self.engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
        item_list = [tuple(row) for row in result] 

        duplicates = None
        if check_for_duplicates == "first_arg":
            counter = Counter(item[0] for item in item_list)
            duplicates = [val for val, count in counter.items() if count > 1]
        elif check_for_duplicates == "all_args":
            duplicates = [item for item, count in Counter(item_list).items() if count > 1]
        
        if duplicates:
            logger_database.warning(f"Duplicates found reading {column_names} from '{table_name}'.")

        return item_list, duplicates


    def remove_duplicates_by_latest_update(
        self,
        table: Table,
        table_name : str,
        conn: Connection,
        df: pd.DataFrame,
        key_column: str = "name",
        date_column: str = "maj_date"
    ) -> pd.DataFrame:
        """
        Remove duplicates from a table based on a key column,
        keeping only the most recent entry according to a date column.

        Parameters:
            table (Table): SQLAlchemy Table object.
            conn (Connection): SQLAlchemy Connection object.
            df (pd.DataFrame): DataFrame representing the table's content.
            key_column (str): Column to identify duplicates (e.g., 'name').
            date_column (str): Column used to determine the most recent row (e.g., 'maj_date').

        Returns:
            pd.DataFrame: Cleaned DataFrame (without duplicates).
        """
        if key_column not in df.columns or date_column not in df.columns:
            logger_database.debug(f"No column called {key_column} or {date_column} in table '{table_name}. Ignoring duplicate deletion.")
            return df

        # Detect duplicates
        duplicates = df[df.duplicated(subset=key_column, keep=False)]
        if duplicates.empty:
            logger_database.debug(f"No duplicates in table '{table_name}'.")
            return df
        logger_database.warning(f"Duplicates detected in table '{table_name}': {set(duplicates['name'])}. Deleting...")

        # Keep only the most recent row for each duplicate key
        latest_rows = (
            duplicates
            .sort_values(date_column, ascending=False)
            .drop_duplicates(subset=key_column, keep='first')
        )

        duplicate_keys = duplicates[key_column].unique().tolist()

        # Delete all duplicate entries from the database and reinsert the latest
        conn.execute(
            delete(table).where(table.c[key_column].in_(duplicate_keys))
        )
        for _, row in latest_rows.iterrows():
            conn.execute(insert(table).values(**row.to_dict()))

        # Return a refreshed dataframe with duplicates removed
        remaining = df[~df[key_column].isin(duplicate_keys)]
        cleaned_df = pd.concat([remaining, latest_rows], ignore_index=True)
        logger_database.info("Duplicates successfully deleted.")

        return cleaned_df


    async def update_markets_and_asset_types(
        self,
        markets: Optional[List[MarketInfo]] = None, # can be used to update a single market
        supported_types : Optional[List[AssetType]] = None,
        strict_deletion: bool = False
    ):
        with self.engine.begin() as conn:
            if supported_types:
                """Update asset types."""
                table_name = "AssetTypes"
                table: Table = self.check_table(table_name=table_name)
                df_asset_types: pd.DataFrame = self.read_table_to_df(specified_table=table)

                df_asset_types: pd.DataFrame = self.remove_duplicates_by_latest_update(
                    table=table,
                    table_name=table_name,
                    conn=conn,
                    df=df_asset_types
                )

                type_ids_in_db = set(df_asset_types['type_id'])
                for asset_type in supported_types:
                    if asset_type.type_id not in type_ids_in_db:
                        """Add each new asset type (unknown id)."""
                        conn.execute(insert(table).values(
                            name=asset_type.name,
                            type_id=asset_type.type_id,
                            maj_date=datetime.now(timezone.utc).replace(microsecond=0)
                            ))
                    else:
                        """Get updates of each already known market and update if needed."""
                        update_dict : Dict[str,Any] = {}
                        line_associated_in_db = df_asset_types.loc[df_asset_types['type_id'] == asset_type.type_id].iloc[0]
                        if asset_type.name != line_associated_in_db.loc["name"]:
                            update_dict["name"] = asset_type.name
                        if update_dict:
                            logger_database.info(f"Updating asset_type name to '{asset_type.name}'.")
                            update_dict["maj_date"] = datetime.now(timezone.utc).replace(microsecond=0)
                            conn.execute(
                                update(table)
                                .where(table.c.type_id == asset_type.type_id)
                                .values(**update_dict)
                            )
                
                if strict_deletion:
                    """Delete obsolet asset types."""
                    input_type_ids = {st.type_id for st in supported_types}
                    ids_to_delete = type_ids_in_db - input_type_ids
                    if ids_to_delete:
                        conn.execute(
                            delete(table)
                            .where(table.c.type_id.in_(ids_to_delete))
                        )
                        logger_database.info("Unknown asset types successfully deleted.")

            if markets:
                """Update markets."""
                table_name = "Markets"
                table: Table = self.check_table(table_name=table_name)
                df_markets: pd.DataFrame = self.read_table_to_df(specified_table=table)

                cross_mrk_type_ass_table: Table = self.check_table(table_name="MarketAssetTypes")
                df_cross_mrk_type_ass: pd.DataFrame = self.read_table_to_df(specified_table=cross_mrk_type_ass_table)
                
                df_markets: pd.DataFrame = self.remove_duplicates_by_latest_update(
                    table=table,
                    table_name=table_name,
                    conn=conn,
                    df=df_markets
                )
                
                market_ids_in_db = set(df_markets['market_id'])           
                for market in markets:
                    if market.market_id not in market_ids_in_db:
                        """Add each new market (unknown id)."""
                        conn.execute(insert(table).values(
                            market_id=market.market_id,
                            name=market.name,
                            asset_number=market.asset_number,
                            status=market.status,
                            website=market.website,
                            maj_date=datetime.now(timezone.utc).replace(microsecond=0)))
                        
                        """Update the MarketAssetTypes cross-table of unknown markets."""
                        asset_types_id_to_insert = [{"market_id" : market.market_id, "type_id": type_id} for type_id in market.type_ids]
                        conn.execute(
                            insert(cross_mrk_type_ass_table),
                            asset_types_id_to_insert
                        )
                        
                    else:
                        """Get updates of each already known market and update if needed."""
                        update_dict : Dict[str,Any] = {}
                        line_associated_in_db = df_markets.loc[df_markets['market_id'] == market.market_id].iloc[0]
                        if market.name != line_associated_in_db.loc["name"]:
                            update_dict["name"] = market.name
                        if market.website != line_associated_in_db.loc["website"]:
                            update_dict["website"] = market.website
                        if market.asset_number != line_associated_in_db.loc["asset_number"]:
                            update_dict["asset_number"] = market.asset_number
                        if market.status != line_associated_in_db.loc["status"]:
                            update_dict["status"] = market.status
                        
                        if update_dict:
                            logger_database.info(f"Updating market '{market.name}' fields : {', '.join(list(update_dict.keys()))}.")
                            update_dict["maj_date"] = datetime.now(timezone.utc).replace(microsecond=0)
                            conn.execute(
                                update(table)
                                .where(table.c.market_id == market.market_id)
                                .values(**update_dict)
                            )

                        """Update the MarketAssetTypes cross-table of already known market."""
                        type_ids_of_market_in_db = df_cross_mrk_type_ass.loc[df_cross_mrk_type_ass['market_id'] == market.market_id, 'type_id'].tolist()
                        insert_dict : List[Dict[str, str]] = []
                        for type_id in market.type_ids:
                            if type_id not in type_ids_of_market_in_db:
                                insert_dict.append({"market_id":market.market_id, "type_id":type_id})
                        if insert_dict:
                            if self.write_df(df=pd.DataFrame(insert_dict), table_name="MarketAssetTypes"):
                                logger_database.info(f"Added asset type ids '{', '.join([line.get('type_id','') for line in insert_dict])} for market '{market.name}'.")


                        obsolet_type_ids_in_curent_market = [type_id for type_id in type_ids_of_market_in_db if type_id not in market.type_ids]
                        if obsolet_type_ids_in_curent_market:
                            conn.execute(
                                delete(cross_mrk_type_ass_table)
                                .where(
                                    and_(
                                        cross_mrk_type_ass_table.c.market_id == market.market_id,
                                        cross_mrk_type_ass_table.c.type_id.in_(obsolet_type_ids_in_curent_market)
                                    )
                                )
                            )
                            logger_database.info(f"Deleted asset type ids '{', '.join(obsolet_type_ids_in_curent_market)} for market '{market.name}.")

                if strict_deletion:
                    """Delete obsolet markets from Markets & Market cross-table."""
                    input_market_ids = {m.market_id for m in markets}
                    markets_to_delete = market_ids_in_db - input_market_ids
                    if markets_to_delete:
                        conn.execute(
                            delete(table)
                            .where(table.c.market_id.in_(markets_to_delete))
                        )
                        conn.execute(
                            delete(cross_mrk_type_ass_table)
                            .where(cross_mrk_type_ass_table.c.market_id.in_(markets_to_delete))
                        )
                        logger_database.info("Unknown markets successfully deleted.")
        

    async def update_assets(
        self,
        asset_type_id: str,
        assets_by_markets: Dict[str,List[BaseAsset]],
        asset_number_limit: int = 200
    ):
        
        registered_at = ASSET_TYPE_RGSTR.get(asset_type_id, None)
        if not registered_at:
            raise AssetTypeNameError
        
        type_cls = registered_at.cls # asset type class
        referent_markets = registered_at.referent_markets
        table_name = type_cls.__name__+"s"
        spec_at_table: Table = self.check_table(table_name=table_name)
        assets_table: Table = self.check_table(table_name='Assets')
        asset_markets_table: Table = self.check_table(table_name='AssetMarkets')
        
        with self.engine.begin() as conn:
            for mrk_id, assets in assets_by_markets.items():

                referent_markets_order = {m: i for i, m in enumerate(referent_markets)} # for referent markets

                sorted_assets = sorted(assets, key=lambda a: int(a.status))[::-1]
                ranked_assets = [el for el in sorted_assets if el.status>=0]
                excess_assets = [el for el in sorted_assets if el.status==-1]
                if len(ranked_assets)>asset_number_limit:
                    ranked_assets, excess_assets = ranked_assets[:asset_number_limit], excess_assets+ranked_assets[asset_number_limit:]
                for asset in ranked_assets:
                    try:
                        with conn.begin_nested():
                            if not isinstance(asset, type_cls):
                                continue

                            existing_asset = conn.execute(
                                select(assets_table).where(assets_table.c.asset_id == asset.asset_id)
                            ).first()

                            if existing_asset:
                
                                current_main_market = existing_asset.main_market_id
                                new_main_market = current_main_market

                                if referent_markets_order.get(mrk_id, len(referent_markets)) < \
                                    referent_markets_order.get(current_main_market, len(referent_markets)):
                                    new_main_market = mrk_id

                                    # -- ASSETS table
                                    stmt = (
                                        update(assets_table)
                                        .where(assets_table.c.asset_id == asset.asset_id)
                                        .values(
                                            status=asset.status,
                                            maj_date=datetime.now(timezone.utc).replace(microsecond=0),
                                            main_market_id=new_main_market,
                                            name=asset.name,
                                            symbol=asset.symbol,
                                            website=asset.website
                                        )
                                    )
                                    conn.execute(stmt)
                                else:
                                    # Do nothing if a better referent market is already in db
                                    pass

                            else:

                                # -- ASSETS table
                                stmt = insert(assets_table).values(
                                    asset_id=asset.asset_id,
                                    symbol=asset.symbol,
                                    name=asset.name,
                                    main_market_id=mrk_id,
                                    type_id=asset_type_id,
                                    status=asset.status,
                                    website=asset.website,
                                    maj_date=datetime.now(timezone.utc).replace(microsecond=0)
                                )
                                conn.execute(stmt)

                            # -- SPEC ASSETS table
                            if isinstance(asset, Crypto):
                                crypto_stmt = insert(spec_at_table).values(
                                    asset_id=asset.asset_id,
                                    quote_asset=asset.quote_asset,
                                    base_asset=asset.base_asset
                                ).on_conflict_do_nothing(
                                    index_elements=["asset_id"]
                                )
                                conn.execute(crypto_stmt)

                            elif isinstance(asset,Future):
                                pass

                            # [NOT IMPLEMENTED] all the other types

                            # -- CROSS ASSET x MARKETS table
                            asset_market_stmt = insert(asset_markets_table).values(
                                asset_id=asset.asset_id,
                                market_id=mrk_id
                            ).on_conflict_do_nothing(
                                index_elements=["asset_id", "market_id"]
                            )
                            conn.execute(asset_market_stmt)

                    except Exception as e:
                        logger_database.error(f"Details: {str(e)}")

                # 0 updates and -1 deletions
                for asset in excess_assets:
                    try:
                        with conn.begin_nested():
                            if asset.status >= 0:
                                stmt = (
                                    update(assets_table)
                                    .where(assets_table.c.asset_id == asset.asset_id)
                                    .values(
                                        status=0,
                                        maj_date=datetime.now(timezone.utc).replace(microsecond=0)
                                    )
                                )
                                conn.execute(stmt)
                            
                            elif asset.status == -1:
                                stmt_am = delete(asset_markets_table).where(asset_markets_table.c.asset_id == asset.asset_id)
                                stmt_crypto = delete(spec_at_table).where(spec_at_table.c.asset_id == asset.asset_id)
                                stmt_asset = delete(assets_table).where(assets_table.c.asset_id == asset.asset_id)
                                for stmt in [stmt_am, stmt_crypto, stmt_asset]:
                                    conn.execute(stmt)
                    except Exception as e:
                        logger_database.error(f"Details: {str(e)}")


    def update_single_asset_infos(
        self,
        market_info : BaseAsset
    ):
        # update either website or name
        pass


    def write_df(self, df: pd.DataFrame, table_name: str):
        try:
            if df.empty:
                logger_database.warning("Dataframe empty, skipping write_df.")
                return
            table = self.check_table(table_name)
            records = df.to_dict(orient="records")
            stmt = insert(table).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["asset_id", "time_frame", "open_time"]
            )

            with self.engine.begin() as conn:
                row_nb = conn.execute(stmt).rowcount or 0
        
            if row_nb == 0:
                logger_database.info(f"Nothing to add to {table_name}.")
            else:
                logger_database.info(f"Data successfully written in {table_name} ({row_nb} rows).")

        except Exception as e:
            logger_database.rooted_exception(f"Details: {str(e)}")
        
        return




