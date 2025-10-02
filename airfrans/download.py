# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests",
#     "tqdm",
# ]
# ///

import requests
import zipfile
from pathlib import Path
from tqdm import tqdm


def download_and_extract(
    zip_file: str, url: str, extracted_dir: str, description: str
) -> None:
    """Download and extract dataset if not already present.

    Args:
        zip_file: Local filename for the downloaded zip file
        url: URL to download the dataset from
        extracted_dir: Directory name where the dataset will be extracted
        description: Human-readable description for progress messages
    """
    zip_path = Path(zip_file)
    extracted_path = Path(extracted_dir)

    # Check if we need to download
    if not zip_path.exists() and not extracted_path.exists():
        print(f"Downloading {description}...")

        # Get file size for progress bar
        response = requests.head(url)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))

        # Download with progress bar
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(zip_path, "wb") as file:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=description,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        pbar.update(len(chunk))
    else:
        print(f"{description} already exists, skipping download.")

    # Extract if zip file exists
    if zip_path.exists():
        print(f"Extracting {description}...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall()
        zip_path.unlink()  # Remove the zip file after extraction


### Main execution
print("Downloading AirfRANS dataset...")

# Download and extract main dataset
download_and_extract(
    zip_file="dataset.zip",
    url="https://data.isir.upmc.fr/extrality/NeurIPS_2022/Dataset.zip",
    extracted_dir="Dataset",
    description="main dataset",
)

# Uncomment the following to also download OpenFOAM dataset
download_and_extract(
    zip_file="of_dataset.zip",
    url="https://data.isir.upmc.fr/extrality/NeurIPS_2022/OF_dataset.zip",
    extracted_dir="OF_Dataset",
    description="OpenFOAM dataset",
)

print("Dataset download and extraction complete!")
