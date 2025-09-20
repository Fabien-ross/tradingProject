import asyncio
from datetime import datetime, timezone

from src.processes.training.training_orchestrator import TrainingOrchestrator
from src.processes.production.production_orchestrator import ProductionOrchestrator

async def training_pipeline():

    # -- Parameters
    ass_id_list = ["crypto-PEOPLEUSDC","crypto-HUMAUSDC"]
    latest_time = "2025-07-20 00:00:00"
    limit_number = 2000

    pd_orch = ProductionOrchestrator()
    training_orch = TrainingOrchestrator(asset_ids=ass_id_list)

    if not await pd_orch.check_and_update_markets():
        return
    
    latest_time = datetime.strptime(latest_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    if not await training_orch.get_historical(
        kline_count=limit_number,
        data_table_name="TrainingData",
        latest_time=latest_time,
        from_scratch=False
    ):
        return

    await training_orch.display()
    


if __name__=="__main__":
    asyncio.run(training_pipeline())


