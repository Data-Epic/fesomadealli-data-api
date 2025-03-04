import os
from processor import clean
import pandas as pd
import polars as pl

df_pandas_clean = clean.clean_pandas(df=clean.df_pandas)
df_polars_clean = clean.clean_polars(df=clean.df_polars)

# columns
group_col_present: bool
agg_col_present: bool
msg: str


def aggregate_pandas(df, group_col, agg_col):
    # Initialize variables inside the function
    group_col_present = True
    agg_col_present = True
    msg = ""

    if group_col not in df.columns:
        group_col_present = False
        msg = f"{group_col} not found in dataframe"
    if agg_col not in df.columns:
        agg_col_present = False
        msg = f"{agg_col} not found in dataframe"

    # Proceed to aggregate
    if (group_col_present) and (agg_col_present) is False:
        msg = f"{group_col} and {agg_col} not in dataframes"
        print(msg)
        return msg
    elif (group_col_present) and (agg_col_present) is True:
        return (
            df.groupby(group_col)[agg_col].agg(["mean", "sum", "count"]).reset_index()
        )
    else:
        return msg


def aggregate_polars(df, group_col, agg_col):
    # Initialize variables inside the function
    group_col_present = True
    agg_col_present = True
    msg = ""

    if group_col not in df.columns:
        group_col_present = False
        msg = f"{group_col} not found in dataframe"

    if agg_col not in df.columns:
        agg_col_present = False
        msg = f"{agg_col} not found in dataframe"

    # Proceed to aggregate
    if (group_col_present and agg_col_present) is False:
        msg = f"{group_col} and {agg_col} not in dataframes"
        print(msg)
        return msg

    elif (group_col_present and agg_col_present) is True:
        return df.group_by(group_col).agg(
            [
                pl.col(agg_col).mean().alias("mean"),
                pl.col(agg_col).sum().alias("sum"),
                pl.col(agg_col).count().alias("count"),
            ]
        )

    else:
        return msg


if __name__ == "__main__":
    aggregate_pandas(df=df_pandas_clean, group_col="Country", agg_col="Quantity")
    aggregate_polars(df=df_polars_clean, group_col="Country", agg_col="Quantity")
