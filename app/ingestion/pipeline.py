from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.services.coinpaprika import fetch_coin_ticker
from app.services.coingecko import fetch_coingecko_data
from app.models import RawData, CryptoMarketData

# --- 1. Define the Identity Map (The Fix for Normalization) ---
# This ensures we unify identity across sources.
COIN_IDENTITY_MAP = {
    "bitcoin": {"symbol": "BTC", "name": "Bitcoin"},
    "ethereum": {"symbol": "ETH", "name": "Ethereum"},
    "solana": {"symbol": "SOL", "name": "Solana"}
}

def normalize_and_store_coingecko(db: Session, coin_id: str, raw_data: dict):
    """
    Dynamic normalization based on the input coin_id.
    """
    # Look up the correct symbol/name from our map
    identity = COIN_IDENTITY_MAP.get(coin_id)
    
    if not identity:
        print(f"Skipping unknown coin: {coin_id}")
        return

    clean_record = CryptoMarketData(
        symbol=identity["symbol"],  # Uses "BTC" for bitcoin, "ETH" for ethereum
        name=identity["name"],
        price_usd=raw_data.get("usd"),
        market_cap_usd=raw_data.get("usd_market_cap"),
        volume_24h_usd=raw_data.get("usd_24h_vol"),
        source="coingecko"
    )
    
    db.add(clean_record)
    db.commit()

def process_pipeline():
    db = SessionLocal()
    try:
        # --- Process CoinPaprika (Existing logic is fine) ---
        print("--- Processing CoinPaprika ---")
        cp_data = fetch_coin_ticker("btc-bitcoin")
        if cp_data:
            db.add(RawData(source="coinpaprika", endpoint="tickers", raw_payload=cp_data))
            # Paprika gives us the symbol directly, so we use it
            db.add(CryptoMarketData(
                symbol=cp_data.get("symbol", "BTC"),
                name=cp_data.get("name", "Bitcoin"),
                price_usd=cp_data.get("quotes", {}).get("USD", {}).get("price"),
                market_cap_usd=cp_data.get("quotes", {}).get("USD", {}).get("market_cap"),
                volume_24h_usd=cp_data.get("quotes", {}).get("USD", {}).get("volume_24h"),
                source="coinpaprika"
            ))
            db.commit()
            print(" -> CoinPaprika BTC Saved")

        # --- Process CoinGecko (The Fixed Logic) ---
        print("--- Processing CoinGecko ---")
        # We now loop through our map to fetch multiple coins dynamically
        for coin_id in ["bitcoin", "ethereum"]: 
            cg_data = fetch_coingecko_data(coin_id)
            if cg_data:
                # Save Raw Audit Trail
                db.add(RawData(source="coingecko", endpoint=f"simple/price/{coin_id}", raw_payload=cg_data))
                # Normalize dynamically
                normalize_and_store_coingecko(db, coin_id, cg_data)
                print(f" -> CoinGecko {coin_id} Saved")
            
    except Exception as e:
        print(f"Pipeline Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    process_pipeline()