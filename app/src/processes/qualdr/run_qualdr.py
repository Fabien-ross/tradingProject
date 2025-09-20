import asyncio
from src.processes.qualdr.qualdr_orchestrator import QualDrOrchestrator


async def qualdr_pipeline():

    # -- Parameters
    
    
    spo_qualdr_orch = QualDrOrchestrator()

    # -- Pipeline
    await spo_qualdr_orch.check()



if __name__=="__main__":
    asyncio.run(qualdr_pipeline())


