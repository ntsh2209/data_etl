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
