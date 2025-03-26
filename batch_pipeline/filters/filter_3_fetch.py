import pandas as pd
import requests
from bs4 import BeautifulSoup
from config import HISTORICAL_DATA_URL, HEADERS, DATE_FORMAT
from datetime import datetime, timedelta
import time

def download_and_format(issuer_code, start_date):
    """Fetch real historical data from MSE"""
    try:
        # Prepare date range (last 30 days as example)
        end_date = datetime.now()
        start_date = datetime.strptime(start_date, DATE_FORMAT)
        
        # Simulate form submission
        form_data = {
            'symbol': issuer_code,
            'from_day': start_date.day,
            'from_month': start_date.month,
            'from_year': start_date.year,
            'to_day': end_date.day,
            'to_month': end_date.month,
            'to_year': end_date.year,
            'submit': 'Прикажи'
        }
        
        response = requests.post(
            HISTORICAL_DATA_URL,
            data=form_data,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        # Parse the HTML table
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'table'})
        
        if not table:
            return pd.DataFrame()
            
        # Extract table data
        rows = []
        for tr in table.find_all('tr')[1:]:  # Skip header
            cols = tr.find_all('td')
            if len(cols) >= 8:  # Ensure we have enough columns
                try:
                    rows.append({
                        'date': cols[0].text.strip(),
                        'price': float(cols[2].text.replace(',', '')),
                        'volume': int(cols[7].text.replace(',', '')),
                        'issuer_code': issuer_code
                    })
                except ValueError:
                    continue
                    
        df = pd.DataFrame(rows)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True).dt.strftime(DATE_FORMAT)
        return df
        
    except Exception as e:
        print(f"Error fetching {issuer_code}: {str(e)}")
        return pd.DataFrame()