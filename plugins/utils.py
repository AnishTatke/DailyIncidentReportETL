import os
import pandas as pd
import numpy as np
import logging
import asyncio
from geopy.geocoders import Nominatim, GeocoderNotFound
from geopy.extra.rate_limiter import AsyncRateLimiter
from geopy.adapters import AioHTTPAdapter

from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable


def save_csv(df: pd.DataFrame, columns: list[str], file_path:str, replacement: tuple[str, str]):
    new_file_path = file_path.replace(*replacement)
    df = pd.DataFrame(df, columns=columns)
    df.to_csv(new_file_path, index=False)
    if os.path.exists(new_file_path):
        logging.info(f"File saved successfully at {new_file_path}")
        os.remove(file_path)
    return new_file_path
    
async def geocode_location(geocode, location: str, attempt: int = 1, max_attempts: int = 5):
    try:
        result = await geocode(location)
        if result is None:
            return location, np.nan, np.nan
        return location, result.latitude, result.longitude
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable):
        if attempt < max_attempts:
            await asyncio.sleep(1)
            return await geocode_location(geocode, location, attempt + 1)
        else:
            return location, np.nan, np.nan

async def get_lat_long(df: pd.DataFrame):
    unique_locations = df['Location'].dropna().unique().tolist()

    async with Nominatim(user_agent='myGeocoder', timeout=10, adapter_factory=AioHTTPAdapter) as geolocator:
        geocode = AsyncRateLimiter(geolocator.geocode, min_delay_seconds=1)
        tasks = [geocode_location(geocode, loc) for loc in unique_locations]
        results = await asyncio.gather(*tasks)

    return {loc: (lat, long) for loc, lat, long in results}