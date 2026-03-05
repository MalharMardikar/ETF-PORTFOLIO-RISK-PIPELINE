import yfinance as yf
import boto3
from pathlib import Path
from datetime import datetime, timedelta

TICKERS = ['AGG', 'GLD', 'IWM', 'QQQ', 'SPY', 'TLT', 'VTI']
END_DATE = datetime.today().strftime('%Y-%m-%d')
START_DATE = (datetime.today() - timedelta(days=5*365)).strftime('%Y-%m-%d')
OUTPUT_FILENAME = 'historical_prices.csv'

S3_BUCKET = 'malharportfolioriskanalysis'
S3_KEY = 'Data/historical_prices.csv'

DATA_DIR = Path(__file__).resolve().parent / 'Data'
DATA_DIR.mkdir(exist_ok=True)

def download_prices():
    data = yf.download(TICKERS, start=START_DATE, end=END_DATE, progress=False)
    closes = data['Close']
    output_path = DATA_DIR / OUTPUT_FILENAME
    closes.to_csv(output_path)
    return output_path

def upload_to_s3(file_path):
    s3_client = boto3.client('s3')
    s3_client.upload_file(str(file_path), S3_BUCKET, S3_KEY)

def main():
    try:
        output_path = download_prices()
        upload_to_s3(output_path)
        print("Success! Pipeline step 1 complete.")
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == '__main__':
    main()
