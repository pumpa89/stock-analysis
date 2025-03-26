from filters.filter_1_scrape import fetch_issuers
from batch_pipeline.filters.filter_2_datecheck import get_last_date
from filters.filter_3_fetch import download_and_format
from database import Database
import time

from config import REQUEST_DELAY

def run_pipeline():
    # ...
    for issuer in issuers:
        time.sleep(REQUEST_DELAY)  # Avoid getting blocked

def run_pipeline():
    issuers = fetch_issuers()
    if not issuers:
        print("⚠️ Warning: Using fallback issuers")
        issuers = ['ALK', 'KMB', 'MPT']  # Minimal working set
    
    # Rest of your pipeline...

def run_pipeline():
    try:
        # Safely get issuers
        issuers = fetch_issuers() or []  # Ensures issuers is never None
        print(f"DEBUG: Found {len(issuers)} issuers: {issuers}")
        
        if not issuers:
            raise ValueError("No issuers found - check scraping configuration")
            
        db = Database()
        
        for issuer in issuers:
            last_date = get_last_date(issuer)
            print(f"Processing {issuer}, last date: {last_date}")
            
            start_date = last_date or "2013-01-01"
            data = download_and_format(issuer, start_date)
            
            if not data.empty:
                print(f"Saving {len(data)} rows for {issuer}")
                db.save_data(data)
            else:
                print(f"No data for {issuer}")
                
        db.close()
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
def run_pipeline():
    db = Database()
    issuers = fetch_issuers()
    print(f"DEBUG: Found {len(issuers)} issuers: {issuers}")  # <-- ADD THIS
    
    for issuer in issuers:
        last_date = get_last_date(issuer)
        print(f"DEBUG: Processing {issuer}, last date: {last_date}")  # <-- ADD THIS
        
        start_date = last_date or "2013-01-01"
        data = download_and_format(issuer, start_date)
        print(f"DEBUG: Data to insert:\n{data}")  # <-- ADD THIS (SHOWS ACTUAL DATA)
        
        db.save_data(data)
        print(f"DEBUG: Saved {len(data)} rows for {issuer}")  # <-- ADD THIS
    
    db.close()
    
    



if __name__ == "__main__":
    start_time = time.time()
    run_pipeline()
    print(f"Pipeline completed in {time.time() - start_time:.2f} seconds.")