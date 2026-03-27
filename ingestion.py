import os
import pandas as pd
from spacetrack import SpaceTrackClient
import io
import time
from datetime import datetime, timedelta

# Configuration paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(PROJECT_ROOT, "isolation", "latest_satellites.parquet")
TLE_TXT_PATH = os.path.join(PROJECT_ROOT, "visualisation", "satellites.txt")
CACHE_EXPIRY_HOURS = 24

# Use environment variables for authentication (for GitHub Actions)
username = os.environ.get('SPACE_TRACK_USERNAME', 's.paresh.ug23@nsut.ac.in')
password = os.environ.get('SPACE_TRACK_PASSWORD', 'P2a0r0e5shspace')

st = SpaceTrackClient(identity=username, password=password)

def is_fetch_too_soon():
    """Checks if the data was fetched recently enough to skip a new fetch."""
    # If any output file is missing, we must fetch
    if not os.path.exists(PARQUET_PATH) or not os.path.exists(TLE_TXT_PATH):
        return False
    
    file_time = datetime.fromtimestamp(os.path.getmtime(PARQUET_PATH))
    if datetime.now() - file_time < timedelta(hours=CACHE_EXPIRY_HOURS):
        return True
    return False

def fetch_daily_tles():
    if is_fetch_too_soon():
        print(f"Data is still fresh (less than {CACHE_EXPIRY_HOURS}h old). Skipping fetch.")
        return pd.read_parquet(PARQUET_PATH)

    print("Fetching data from Space-Track...")

    # Query the 'gp' class
    # gp contains General Perturbation (GP) data (improved TLEs)
    try:
        data = st.gp(format='csv')
    except Exception as e:
        print(f"Error fetching data from Space-Track: {e}")
        # If fetch fails, try to return existing data if available
        if os.path.exists(PARQUET_PATH):
            print("Using existing local data as fallback.")
            return pd.read_parquet(PARQUET_PATH)
        raise

    # Load into DataFrame using io.StringIO
    df = pd.read_csv(io.StringIO(data))

    # Clean and filter columns
    columns_to_keep = [
        'NORAD_CAT_ID', 'OBJECT_NAME', 'EPOCH', 'INCLINATION',
        'ECCENTRICITY', 'MEAN_MOTION', 'BSTAR', 'TLE_LINE1', 'TLE_LINE2'
    ]

    # Standardize column names to uppercase
    df.columns = [c.upper() for c in df.columns]
    cols_present = [c for c in columns_to_keep if c in df.columns]
    df_clean = df[cols_present].copy()

    # Save as Parquet for the R pipeline (isolation/ directory)
    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
    df_clean.to_parquet(PARQUET_PATH)
    print(f"Saved {len(df_clean)} satellites to {PARQUET_PATH}")

    # Generate satellites.txt for the frontend (visualisation/ directory)
    # TLE_LINE1 and TLE_LINE2 should alternate lines
    os.makedirs(os.path.dirname(TLE_TXT_PATH), exist_ok=True)
    with open(TLE_TXT_PATH, "w") as f:
        # We only need the TLE lines for the viewer
        for _, row in df_clean.iterrows():
            f.write(f"{row['TLE_LINE1']}\n")
            f.write(f"{row['TLE_LINE2']}\n")
    print(f"Saved TLE data to {TLE_TXT_PATH}")

    return df_clean

if __name__ == "__main__":
    df_final = fetch_daily_tles()
    print(f"Successfully processed {len(df_final)} satellites.")