"""
Pipeline complet :
1. Collecte des données (Binance, CoinGecko, Kraken)
2. Sauvegarde raw JSON
3. Bronze Layer (Spark)
4. Clean Layer (Spark)
5. Silver Layer (Spark)
6. Gold Layer (Spark)
7. Dashboard (Streamlit)
"""

import subprocess
import sys
import os
from ingestion.binance import get_binance_data
from ingestion.producer_rest.coingecko_market_snapshot import get_coingecko_data
from ingestion.kraken import get_kraken_data
from storage.save import save_raw

def run_pipeline():

    # ── ÉTAPE 1 : COLLECTE DES DONNÉES ───────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 1 — Collecte des données")
    print("="*50)

    print("Collecting data from Binance API...")
    binance = get_binance_data()

    print("Collecting data from CoinGecko API...")
    coingecko = get_coingecko_data()

    print("Collecting data from Kraken API...")
    kraken = get_kraken_data()

    print("✅ Collecte terminée !")

    # ── ÉTAPE 2 : SAUVEGARDE RAW JSON ─────────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 2 — Sauvegarde raw JSON")
    print("="*50)

    save_raw(binance,   "raw_data/binance_raw.json")
    save_raw(coingecko, "raw_data/coingecko_raw.json")
    save_raw(kraken,    "raw_data/kraken_raw.json")

    print("✅ Raw data sauvegardée !")

    # ── ÉTAPE 3 : BRONZE LAYER ────────────────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 3 — Bronze Layer")
    print("="*50)

    result = subprocess.run(
        [sys.executable, "spark/bronze.py"],
        capture_output=False
    )
    if result.returncode != 0:
        print("❌ Erreur dans bronze.py !")
        sys.exit(1)
    print("✅ Bronze Layer créé !")

    # ── ÉTAPE 4 : CLEAN LAYER ─────────────────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 4 — Clean Layer")
    print("="*50)

    result = subprocess.run(
        [sys.executable, "spark/clean.py"],
        capture_output=False
    )
    if result.returncode != 0:
        print("❌ Erreur dans clean.py !")
        sys.exit(1)
    print("✅ Clean Layer créé !")

    # ── ÉTAPE 5 : SILVER LAYER ────────────────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 5 — Silver Layer")
    print("="*50)

    result = subprocess.run(
        [sys.executable, "spark/silver.py"],
        capture_output=False
    )
    if result.returncode != 0:
        print("❌ Erreur dans silver.py !")
        sys.exit(1)
    print("✅ Silver Layer créé !")

    # ── ÉTAPE 6 : GOLD LAYER ──────────────────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 6 — Gold Layer")
    print("="*50)

    result = subprocess.run(
        [sys.executable, "spark/gold.py"],
        capture_output=False
    )
    if result.returncode != 0:
        print("❌ Erreur dans gold.py !")
        sys.exit(1)
    print("✅ Gold Layer créé !")

    # ── ÉTAPE 7 : DASHBOARD ───────────────────────────────────
    print("\n" + "="*50)
    print("ÉTAPE 7 — Lancement Dashboard")
    print("="*50)
    print("🚀 Dashboard disponible sur http://localhost:8501")

    subprocess.run(
        ["streamlit", "run", "dashboard/app.py"],
        capture_output=False
    )

if __name__ == "__main__":
    run_pipeline()