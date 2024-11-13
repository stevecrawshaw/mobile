
#%%
from pathlib import Path
import requests
import time
import random
import zipfile
import polars as pl
#%%
def download_zip_files(urls: list, output_dir: str = "data") -> None:
    """
    Downloads zip files from a list of URLs and saves them to the output directory.
    
    Args:
        urls (list): List of URLs to download
        output_dir (str): Directory to save the downloaded files (default: 'data')
        
    Returns:
        None
        
    Raises:
        requests.exceptions.RequestException: If there's an error fetching the URLs 
        OSError: If there's an error creating the output directory or saving files
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for url in urls:
            try:
                filename = Path(url).name
                output_file = output_path / filename
                
                print(f"Downloading {filename}...")
                
                # Stream the download to handle large files
                with requests.get(url, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    with open(output_file, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            
                print(f"Successfully downloaded {filename}")
                
                # Add random delay between downloads
                time.sleep(random.uniform(1, 3))
                
            except requests.exceptions.RequestException as e:
                print(f"Error downloading {url}: {str(e)}")
            except OSError as e:
                print(f"Error saving {url}: {str(e)}")
                
    except OSError as e:
        raise OSError(f"Error creating output directory: {str(e)}")

#%%
    # these urls scraped from the Ofcom website
urls = [
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-2020-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-dec20-may21-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-jun21-sep21-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-oct21-dec21-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-jan22-apr22-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-may22-sep22-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-oct22-dec22-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-aug23-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-sep23-mobile-signal-measurement-data.zip",
"https://static.ofcom.org.uk/static/research/mobile-signal-strength-data/5g-nr-oct23-mobile-signal-measurement-data.zip",
]

#%%

if __name__ == "__main__":
    download_zip_files(urls)

# %%
def extract_csvs(folder_path: str = "data") -> int:
    """
    Extracts CSV files from all zip files in the specified folder.
    
    Args:
        folder_path (str): Path to folder containing zip files (default: 'data')
        
    Returns:
        int: 1 if successful, None if failed
        
    Raises:
        OSError: If there's an error accessing the folder or files
    """
    try:
        folder = Path(folder_path)
        if not folder.exists():
            print(f"Folder {folder_path} does not exist")
            return None
            
        zip_files = list(folder.glob('*.zip'))
        if not zip_files:
            print(f"No zip files found in {folder_path}")
            return None
            
        for zip_path in zip_files:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # Get list of CSV files in zip
                    csv_files = [f for f in zip_ref.namelist() if Path(f).suffix.lower() == '.csv']
                    
                    if not csv_files:
                        print(f"No CSV files found in {zip_path.name}")
                        continue
                        
                    # Extract CSV files
                    print(f"Extracting from {zip_path.name}...")
                    for csv_file in csv_files:
                        zip_ref.extract(csv_file, folder)
                        print(f"Extracted {Path(csv_file).name}")
                        raw = pl.scan_csv(f"{folder_path}/{Path(csv_file)}")
                        out_df = (raw
                            .filter(pl.col("latitude").is_between(51.270, 51.9))
                            .filter(pl.col("longitude").is_between(-3.021, -2.252))
                        ).collect()
                        out_df.write_parquet(f"{folder_path}/{Path(csv_file).stem}.parquet")
                        
                        
            except zipfile.BadZipFile as e:
                print(f"Error with zip file {zip_path.name}: {str(e)}")
                continue
            except Exception as e:
                print(f"Error extracting from {zip_path.name}: {str(e)}")
                continue
                
        return 1
        
    except Exception as e:
        print(f"Error processing folder {folder_path}: {str(e)}")
        return None
    
#%%
    
extract_csvs("data")
#%%