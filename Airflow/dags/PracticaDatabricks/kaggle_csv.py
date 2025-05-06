import os
import json

def download_kaggle_dataset():
    base_dir = os.path.dirname(__file__)

    cred_file = os.path.join(base_dir, "credentials", "kaggle_credentials.json")
    with open(cred_file, 'r') as f:
        creds = json.load(f)

    os.environ["KAGGLE_USERNAME"] = creds["username"]
    os.environ["KAGGLE_KEY"]      = creds["key"]

    download_dir = os.path.join(base_dir, "csv")
    os.makedirs(download_dir, exist_ok=True)

    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        "mexwell/open-university-learning-analytics",
        path=download_dir,
        unzip=True
    )