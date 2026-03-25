import os
import pandas as pd
from spacetrack import SpaceTrackClient
import io

# Use environment variables for authentication (for GitHub Actions)
username = os.environ.get('SPACE_TRACK_USERNAME', 's.paresh.ug23@nsut.ac.in')
password = os.environ.get('SPACE_TRACK_PASSWORD', 'P2a0r0e5shspace')

st = SpaceTrackClient(identity=username, password=password)
def fetch_daily_tles():
    print("Fetching data from Space-Track...")

    # 2. Query the 'gp' class
    # Removing iter_lines=True makes it return the full string directly,
    # which is easier for pandas to read.
    data = st.gp(format='csv')

    # 3. Load into DataFrame using io.StringIO
    # We use io.StringIO to make the string behave like a file
    df = pd.read_csv(io.StringIO(data))

    # 4. Filter relevant columns for your Isolation Forest & Visualization
    columns_to_keep = [
        'NORAD_CAT_ID', 'OBJECT_NAME', 'EPOCH', 'INCLINATION',
        'ECCENTRICITY', 'MEAN_MOTION', 'BSTAR', 'TLE_LINE1', 'TLE_LINE2'
    ]

    # Check if columns exist (Space-Track column names are usually uppercase)
    df.columns = [c.upper() for c in df.columns]
    columns_to_keep = [c.upper() for c in columns_to_keep]

    df_clean = df[columns_to_keep]

    return df_clean

# Run the fetch
df_final = fetch_daily_tles()
print(f"Successfully fetched {len(df_final)} satellites.")