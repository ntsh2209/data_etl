import os
from datetime import datetime
from pathlib import Path

def delete_old_parquet_files(cache_dir: str):
    today = datetime.today().date()
    deleted_files = []

    for file in Path(cache_dir).glob("*.parquet"):
        # Get last modified date
        mod_time = datetime.fromtimestamp(file.stat().st_mtime).date()

        if mod_time < today:
            file.unlink()
            deleted_files.append(file.name)

    return deleted_files


from functools import wraps

def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"🔹 Calling: {func.__name__}()")
        logger.info(f"   ↳ args: {args}, kwargs: {kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"✅ {func.__name__}() returned: {result}")
            return result
        except Exception as e:
            logger.exception(f"❌ Exception in {func.__name__}(): {e}")
            raise
    return wrapper
