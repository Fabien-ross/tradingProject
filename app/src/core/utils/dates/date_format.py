from datetime import datetime, timedelta, timezone
from typing import Optional, Dict

"""Authorized time frames"""
interval_map = {
        "5m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1)
    }

def get_unix_time_s(
    count: int, 
    time_frame: str, 
    latest_time: Optional[datetime] = None
) -> tuple[datetime, datetime]:
    """Get the datetime timestamp of the current and past time frame steps."""
    
    if latest_time is None:
        latest_time = datetime.now(timezone.utc).replace(microsecond=0)

    if latest_time.tzinfo is None:
        raise ValueError("latest_time must be timezone-aware (UTC).")

    if time_frame not in interval_map:
        raise ValueError(f"Invalid time_frame: {time_frame}")
    
    if count<1:
        raise ValueError("Count number in get_unix_time_s isn't valid.")
    count = count-1

    # Get duration as timedelta
    time_frame_delta = interval_map[time_frame]
    
    # Floor latest_time to the closest multiple of time_frame
    seconds_since_epoch = int(latest_time.timestamp())
    frame_seconds = int(time_frame_delta.total_seconds())
    floored_epoch = seconds_since_epoch - (seconds_since_epoch % frame_seconds)
    floored_latest_time = datetime.fromtimestamp(floored_epoch, tz=timezone.utc)

    # Compute oldest timestamp
    oldest_time = floored_latest_time - count * time_frame_delta

    return floored_latest_time, oldest_time


def get_all_unix_time_s(
    count: int,
    latest_time: Optional[datetime] = None
) -> Dict[str,tuple[datetime,datetime]]:

    if latest_time is None:
        latest_time = datetime.now(timezone.utc).replace(microsecond=0)
    
    time_dict : Dict[str,tuple[datetime,datetime]] = {}
    for tf in interval_map.keys():
        time_dict[tf] = get_unix_time_s(
            count=count,
            time_frame=tf,
            latest_time=latest_time
            )

    return time_dict

    
def normalize_timestamp_to_seconds(ts: int | float) -> datetime:
    if ts > 1e12:  # millisecondes
        ts /= 1000
    return datetime.fromtimestamp(ts)



