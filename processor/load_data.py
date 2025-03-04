# import statements
import os
import time
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import polars as pl
import requests

filepath: str
url: str

# url to fetch the data from
url = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"


# Navigate up one level from "processor" to "data_api"
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
# Define the correct path to my_files/online_retail_II.xlsx
filepath = os.path.join(ROOT_DIR, "my_files", "online_retail_II.xlsx")


# check file in memory
def file_exists():
    if not os.path.exists(filepath):
        print("File Not Found")
        return False
    else:
        return True


def download_and_extract_data(url):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Define the target directory in the parent folder (data_api)
        target_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir, "my_files")
        )

        # Ensure the directory exists
        os.makedirs(target_dir, exist_ok=True)

        if response.status_code == 200:
            # Create a ZipFile object from the response content
            with ZipFile(BytesIO(response.content)) as my_zip_file:
                # Extract all contents to the current directory
                my_zip_file.extractall("target_dir")
                print("Files extracted successfully")
        else:
            print("Failed to download dataset.")
    except requests.exceptions.HTTPError as e:
        # Capture and print HTTP errors
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        # Catch and print other exceptions
        print(f"Other error occurred: {e}")
    except requests.exceptions.ChunkedEncodingError as e:
        print("ChunkedEncodingError occurred:", e)


# Load Datasets
def load_with_pandas(filepath):
    start_time = time.time()
    df = pd.read_excel(filepath)
    load_time = time.time() - start_time
    return df, load_time


def load_with_polars(filepath):
    start_time = time.time()
    df = pl.read_excel(filepath)
    load_time = time.time() - start_time
    return df, load_time


# Aggregation
def agg_pandas(df):
    start_time = time.time()
    df_grouped = df.groupby("Country")["Quantity"].sum()
    agg_time = time.time() - start_time
    return df_grouped, agg_time


def agg_polars(df):
    start_time = time.time()
    df_grouped = df.group_by("Country").agg(pl.col("Quantity").sum())
    agg_time = time.time() - start_time
    return df_grouped, agg_time


# Memory Usage
def memory_usage_pandas(df):
    return df.memory_usage(deep=True).sum()


def memory_usage_polars(df):
    return df.estimated_size()


# Filtering Columns
def filtering_pandas(df):
    start_time = time.time()
    df_filtered = df[df["Quantity"] > 10]
    filter_time = time.time() - start_time
    return filter_time


def filtering_polars(df):
    start_time = time.time()
    df_filtered = df.filter(pl.col("Quantity") > 10)
    filter_time = time.time() - start_time
    return filter_time


# Benchmark Function
def pandas_versus_polars(filepath):

    df_pandas, pandas_loadtime = load_with_pandas(filepath)
    df_polars, polars_loadtime = load_with_polars(filepath)

    _, pandas_agg_time = agg_pandas(df_pandas)
    _, polars_agg_time = agg_polars(df_polars)

    pandas_memory = memory_usage_pandas(df_pandas)
    polars_memory = memory_usage_polars(df_polars)

    pandas_filter_time = filtering_pandas(df_pandas)
    polars_filter_time = filtering_polars(df_polars)

    benchmark_dict = {
        "Load Time": f"Pandas: {pandas_loadtime:.4f} seconds | Polars: {polars_loadtime:.4f} seconds",
        "Aggregation Time": f"Pandas: {pandas_agg_time:.4f} seconds | Polars: {polars_agg_time:.4f}seconds",
        "Memory Usage": f"Pandas: {pandas_memory / 1e6:.2f} MB | Polars: {polars_memory / 1e6:.2f} MB",
        "Filtering Time": f"Pandas: {pandas_filter_time:.4f} seconds | Polars: {polars_filter_time:.4f}"
    }


    print("\nComparison Results:")
    print(benchmark_dict)

    return benchmark_dict


if __name__ == "__main__":
    if file_exists() is False:
        download_and_extract_data(url=url)
    else:
        pandas_versus_polars(filepath=filepath)
