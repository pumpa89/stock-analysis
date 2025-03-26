import requests
from bs4 import BeautifulSoup
from config import MSE_BASE_URL
import re

def fetch_issuers():
    """Get issuers from the actual page structure"""
    try:
        url = f"{MSE_BASE_URL}/mk/statistiki"
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the symbol dropdown
        symbol_select = soup.find('select', {'name': 'symbol'})
        if symbol_select:
            return [
                opt.text.strip() for opt in symbol_select.find_all('option') 
                if opt.get('value') and not any(c.isdigit() for c in opt.text)
            ]
            
    except Exception as e:
        print(f"Scraping error: {e}")
    
    # Fallback list
    return ["ADIN", "ALK", "CKB", "DEBA", "DIMI",
        "EUHA", "EVRO", "FAKM", "FERS",
        "FUBT", "GALE", "GECK", "GECT", "GIMS", "GRNT", "GRZD",
        "GTC", "GTRG", "INB", "KARO",
        "KDFO", "KJUBI", "KLST", "KMB", "KOMU", "KONF", "KONZ",
        "KPSS", "KVAS", "LOTO", "LOZP", "MAKP", "MAKS",
        "MB", "MERM", "MKSD", "MPOL", "MPT", "MTUR",
        "MZPU", "NEME", "NOSK", "OKTA",
        "OTEK", "PKB", "POPK", "PPIV", "PROD", "RADE",
        "REPL", "RZTK", "RZUG",
        "RZUS", "SBT", "SDOM", "SIL", "SKP", "SLAV", "SOLN",
        "SPAZ", "SPAZP", "STB", "STBP", "STIL", "STOK", "TAJM",
        "TEAL", "TEHN", "TEL", "TETE", "TIKV", "TKPR", "TKVS", "TNB",
        "TRDB", "TRPS",
        "TSMP", "TTK", "UNI", "USJE", "VITA",
        "VROS", "VTKS", "ZAS", "ZILU", "ZILUP", "ZIMS", "ZKAR", "ZPKO"]  # Core stocks