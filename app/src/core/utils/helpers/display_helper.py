import asyncio
import sys
import itertools

async def spinner(stop_event: asyncio.Event):
    for c in itertools.cycle("▖▘▝▗"):
        if stop_event.is_set():
            break
        sys.stdout.write(f"\r{c} Running...")
        sys.stdout.flush()
        await asyncio.sleep(0.2)
    sys.stdout.write("\rStopped.     \n")
    sys.stdout.flush()
