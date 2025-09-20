import asyncio
from src.processes.production.production_orchestrator import ProductionOrchestrator
from src.processes.qualdr.qualdr_orchestrator import QualDrOrchestrator


async def production_pipeline():

    # -- Parameters
    live_asset_number_limit = 40
    kline_count = 200
    
    prod_orch = ProductionOrchestrator()

    # -- Pipeline
    # await prod_orch.DEV_table_rase()
    # if not await prod_orch.launch_db():
    #     return
    if not await prod_orch.check_and_update_markets():
        return
    # if not await prod_orch.update_assets_tables(ass_nb_limit=live_asset_number_limit):
    #     return
    if not await prod_orch.historical_catchup(kline_count=kline_count,data_table_name="LiveData"):
        return

    # await prod_orch.run_ponctual(['5m','15m'])
    await prod_orch.run_ponctuals()

   
if __name__=="__main__":
    asyncio.run(production_pipeline())


