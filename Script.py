import requests
import pandas as pd
import datetime
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException

# Set up a session with retries for robust API calls
session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

# Define protocols (only UNI and AAVE)
protocols = [
    {'slug': 'uniswap', 'token': 'UNI', 'coin_id': 'uniswap'},
    {'slug': 'aave', 'token': 'AAVE', 'coin_id': 'aave'}
]

# Set the date range: last 60 days up to yesterday
end_date = datetime.date.today() - datetime.timedelta(days=1)
start_date = end_date - datetime.timedelta(days=59)
dates = pd.date_range(start=start_date, end=end_date, freq='D')

# Create an empty DataFrame with dates as index
df = pd.DataFrame(index=dates)

# Fetch TVL and price data for each protocol
for protocol in protocols:
    slug = protocol['slug']
    token = protocol['token']
    coin_id = protocol['coin_id']
    
    try:
        # Fetch TVL data from DeFi Llama
        tvl_response = session.get(f'https://api.llama.fi/protocol/{slug}', timeout=30)
        tvl_response.raise_for_status()  # Raise an exception for HTTP errors
        tvl_data = tvl_response.json()['tvl']
        tvl_df = pd.DataFrame(tvl_data)
        tvl_df['date'] = pd.to_datetime(tvl_df['date'], unit='s')
        tvl_df = tvl_df.set_index('date')['totalLiquidityUSD']
        tvl_df.index = tvl_df.index.floor('D')  # Normalize to start of day
        tvl_df = tvl_df.groupby(level=0).last()  # Take last value per day
        
        # Fetch price data from CoinGecko
        price_response = session.get(
            f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart',
            params={'vs_currency': 'usd', 'days': 60},
            timeout=30
        )
        price_response.raise_for_status()
        price_data = price_response.json()['prices']
        price_df = pd.DataFrame(price_data, columns=['timestamp', 'price'])
        price_df['date'] = pd.to_datetime(price_df['timestamp'], unit='ms')
        price_df = price_df.set_index('date')['price']
        price_df.index = price_df.index.floor('D')  # Normalize to start of day
        price_df = price_df.groupby(level=0).last()  # Take last value per day
        
        # Add data to the main DataFrame
        df[f'TVL_{slug}'] = tvl_df
        df[f'price_{token}'] = price_df
        
    except RequestException as e:
        print(f"Error fetching data for {slug}: {e}")
        continue  # Skip to next protocol on error

# Perform correlation analysis
lags = range(-5, 6)  # Lags from -5 to 5 days
correlation_df = pd.DataFrame(index=lags)

for protocol in protocols:
    slug = protocol['slug']
    token = protocol['token']
    
    # Calculate daily changes
    tvl_change = df[f'TVL_{slug}'].diff()
    price_change = df[f'price_{token}'].diff()
    
    # Compute correlations for each lag
    correlations = [
        tvl_change.corr(price_change.shift(lag))
        for lag in lags
    ]
    correlation_df[slug] = correlations

# Save results to CSV files
df.to_csv('data.csv')
correlation_df.to_csv('correlations.csv')

# Print summary of findings
print("Summary of TVL and Price Relationship Patterns:")
for protocol in protocols:
    slug = protocol['slug']
    max_corr_lag = correlation_df[slug].abs().idxmax()
    max_corr = correlation_df[slug].loc[max_corr_lag]
    if max_corr_lag > 0:
        print(f"- {slug}: Highest correlation at lag {max_corr_lag} (correlation: {max_corr:.2f}), "
              f"suggesting price changes lead TVL changes.")
    elif max_corr_lag < 0:
        print(f"- {slug}: Highest correlation at lag {max_corr_lag} (correlation: {max_corr:.2f}), "
              f"suggesting TVL changes lead price changes.")
    else:
        print(f"- {slug}: Highest correlation at lag 0 (correlation: {max_corr:.2f}), "
              f"suggesting a contemporaneous relationship.")