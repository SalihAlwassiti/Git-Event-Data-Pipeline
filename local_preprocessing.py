import os
import subprocess
import urllib.request
import gzip
import shutil
import time
from datetime import datetime, timezone, timedelta

def download_and_extract(target_dir):
    now = datetime.now(timezone.utc)
    target_month = now.month - 1
    target_year = now.year
    if target_month == 0:
        target_month = 12
        target_year -= 1
        
    target_day = now.day
    while True:
        try:
            target_date = now.replace(year=target_year, month=target_month, day=target_day)
            break
        except ValueError:
            target_day -= 1
            
    url = f"https://data.gharchive.org/{target_date.year}-{target_date.month:02d}-{target_date.day:02d}-{target_date.hour}.json.gz"
    
    archive_path = os.path.join(target_dir, "temp_archive.json.gz")
    final_json_path = os.path.join(target_dir, "latest_data.json")

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response, open(archive_path, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            
        with gzip.open(archive_path, 'rb') as f_in:
            with open(final_json_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        os.remove(archive_path)
        
    except urllib.error.HTTPError:
        pass
    except Exception:
        pass

def get_seconds_until_next_5th_minute():
    now = datetime.now(timezone.utc)
    
    if now.minute < 5:
        next_run = now.replace(minute=5, second=0, microsecond=0)
    else:
        next_run = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
        
    return (next_run - now).total_seconds(), next_run

def main():
    target_dir = r"C:\Main\Coding\Code\Pipeline\airbyte_data"
    
    os.makedirs(target_dir, exist_ok=True)
    
    server_process = subprocess.Popen(
        ["python", "-m", "http.server", "8008"], 
        cwd=target_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    tailscale_process = subprocess.Popen(
        ["tailscale", "funnel", "8008"], 
        cwd=target_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    time.sleep(3)
    
    try:
        while True:
            sleep_seconds, next_run_time = get_seconds_until_next_5th_minute()
            time.sleep(sleep_seconds)
            
            download_and_extract(target_dir)
            
    except KeyboardInterrupt:
        server_process.terminate()
        tailscale_process.terminate()

if __name__ == "__main__":
    main()
