# import satements
import os
import pandas as pd
import polars as pl

from processor import aggregate

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
# from fastapi.encoders import jsonable_encoder

global polars_json, polars_parquet
os.makedirs("./my_files", exist_ok=True)
polars_json: dict[str, any]
polars_parquet: dict[str, any]

pandas_agg = aggregate.aggregate_pandas(
    df=aggregate.df_pandas_clean, group_col="Country", agg_col="Quantity"
)
polars_agg = aggregate.aggregate_polars(
    df=aggregate.df_polars_clean, group_col="Country", agg_col="Quantity"
)


app = FastAPI()


@app.get("/")
def landing_page():
    return {"Data Epic Wk-2": "Data API Task"}


@app.get("/benchmark")
def benchmark_polars_vs_pandas():
    benchmark_dict = aggregate.clean.load_data.pandas_versus_polars(
        aggregate.clean.load_data.filepath
    )
    return benchmark_dict


@app.get("/aggregate", response_class=HTMLResponse)
def aggregate_data():
    pandas_to_html = pandas_agg.to_html()
    polars_to_html = polars_agg.to_pandas().to_html()

    html_response = f"""
    <html>
        <body>
            <h2>Pandas Dataframe</h2>
            {pandas_to_html}
            <h2>Polars Dataframe</h2>
            {polars_to_html}
        </body>
    </html>
    """
    return html_response
    # JSON Response
    # return {"Pandas Dataframe": pandas_agg.to_json(), "Polars Dataframe": polars_agg.write_json()}


@app.get("/process-data")
def process_data_to_json():
    # write polars to JSON
    polars_json = polars_agg.write_json()
    # write pandas to JSON
    pandas_json = pandas_agg.to_json()
    # print results on fastapi endpoint
    return {"polars as json": polars_json, "pandas as json": pandas_json}


# Endpoint to generate and serve a JSON file
@app.get("/download-json")
def download_json():
    # Path for the JSON file inside my_files folder
    json_filepath = os.path.join("./my_files", "polars_data.json")
    # Save DataFrame to JSON in /uploads directory
    polars_agg.write_json(json_filepath)
    # Serve the file for download
    return FileResponse(
        path=json_filepath,
        filename="data.json",  # Name for the user
        media_type="application/json",
    )


@app.get("/download-parquet")
# Generate and return a Parquet file.
def download_parquet():
    # Path for the JSON file inside my_files folder
    parquet_filepath = os.path.join("./my_files", "polars_data.json")
    # write polars to Parquet
    polars_agg.write_parquet(parquet_filepath)

    return FileResponse(
        parquet_filepath, filename="data.parquet", media_type="application/octet-stream"
    )
