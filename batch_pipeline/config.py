import os
DATE_FORMAT = os.getenv('DATE_FORMAT', '%Y-%m-%d')

MSE_BASE_URL = "https://www.mse.mk"
HISTORICAL_DATA_URL = f"{MSE_BASE_URL}/mk/statistiki"  # Actual working endpoint
REQUEST_TIMEOUT = 30
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept-Language': 'en-US,en;q=0.5'
}             # Date format for database storage
PRICE_FORMAT = "{:,.2f}"               # Price formatting (e.g., 1,234.56)
REQUEST_TIMEOUT = 30                   # Timeout in seconds