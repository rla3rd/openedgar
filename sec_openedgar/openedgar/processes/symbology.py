import requests
import os
import time
import logging

logger = logging.getLogger(__name__)

class OpenFIGIClient:
    """
    Client for the OpenFIGI API to resolve CUSIP/ISIN/Ticker into FIGI.
    """
    URL = "https://api.openfigi.com/v3/mapping"
    API_KEY = os.getenv("OPENFIGI_API_KEY") # Optional but recommended for higher rates

    @classmethod
    def resolve_batch(cls, identifiers: list, id_type: str = "ID_CUSIP"):
        """
        Resolves a batch of identifiers (max 100 per request).
        id_type can be: ID_CUSIP, ID_ISIN, TICKER
        """
        headers = {"Content-Type": "application/json"}
        if cls.API_KEY:
            headers["X-OPENFIGI-APIKEY"] = cls.API_KEY
            
        # Format for OpenFIGI: [{"idType": "ID_CUSIP", "idValue": "..."}]
        jobs = [{"idType": id_type, "idValue": val} for val in identifiers]
        
        results = []
        try:
            # Batch process in groups of 100
            for i in range(0, len(jobs), 100):
                batch = jobs[i:i+100]
                response = requests.post(cls.URL, json=batch, headers=headers)
                
                if response.status_code == 200:
                    results.extend(response.json())
                elif response.status_code == 429:
                    logger.warning("OpenFIGI Rate Limit hit. Sleeping...")
                    time.sleep(6) # Public rate limit is 10 req/min
                    return cls.resolve_batch(identifiers[i:], id_type)
                else:
                    logger.error(f"OpenFIGI Error {response.status_code}: {response.text}")
                    results.extend([{"error": "API Error"}] * len(batch))
                    
        except Exception as e:
            logger.error(f"OpenFIGI Exception: {e}")
            
        return results

    @classmethod
    def enrich_company_model(cls, company_record):
        """
        Takes a Django Company record and attempts to resolve its FIGI using its CUSIP or Ticker.
        """
        search_val = None
        id_type = None
        
        if company_record.cusip:
            search_val = company_record.cusip
            id_type = "ID_CUSIP"
        elif company_record.ticker:
            search_val = company_record.ticker
            id_type = "TICKER"
            
        if not search_val:
            return False
            
        res = cls.resolve_batch([search_val], id_type=id_type)
        if res and "data" in res[0]:
            match = res[0]["data"][0]
            company_record.figi = match.get("figi")
            company_record.ticker = match.get("ticker", company_record.ticker)
            company_record.exchange = match.get("exchCode", company_record.exchange)
            company_record.save()
            return True
            
        return False
