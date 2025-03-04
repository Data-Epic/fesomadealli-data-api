import os
from processor import load_data
import pandas as pd
import polars as pl

# specify filepath
filepath = load_data.filepath

# check for dataset
if load_data.file_exists() is False:
    load_data.download_and_extract_data(load_data.url)

# load dataset
df_pandas, _ = load_data.load_with_pandas(load_data.filepath)
df_polars, _ = load_data.load_with_polars(load_data.filepath)


def clean_polars(df):
    # inspect for null values in each column
    # print("null values in each column: \n", df.null_count())

    # print the rows in polars where the is at least one null value in any column
    # print(
    #     "rows in polars where there is at least one null value in any column: \n",
    #     df.filter(pl.any_horizontal(pl.col("*").is_null())),
    # )

    # drop all the null values in df_pol using polars
    df_polars_clean = df.drop_nulls()
    # Remove rows where Quantity is +ve
    df_polars_clean = df_polars_clean.filter(pl.col("Quantity") >= 0)
    # Remove letters, keep only digits
    df_polars_clean = df_polars_clean.with_columns(pl.col("StockCode").str.replace_all(r'\D', ''))

    print("Cleaned Polars Dataframe: \n", df_polars_clean)
    return df_polars_clean


def clean_pandas(df):
    # Inspect null rows in df
    # print("null rows in pandas: \n", df.isnull().any(axis=1))

    # Drop rows with null values
    df_pandas_clean = df.dropna()
    # Remove rows where Quantity is +ve
    df_pandas_clean = df_pandas_clean.loc[df_pandas_clean["Quantity"] >= 0]
    # Remove Alphabets from Stockcode column
    df_pandas_clean['StockCode'] = df_pandas_clean['StockCode'].str.replace(r'\D', '', regex=True)

    print("Pandas Cleaned Dataframe: \n", df_pandas_clean)
    return df_pandas_clean


if __name__ == "__main__":
    # clean data
    clean_polars(df=df_polars)
    clean_pandas(df=df_pandas)
