## Project: Data Processing API
Goal:
Build a FastAPI-based web service that processes large datasets using Polars and Pandas. The service should:

Load, clean, and aggregate data.
Expose a FastAPI endpoint to return processed results.
Write results to a JSON or Parquet file.
You will compare Polars and Pandas performance, handle missing values, and test the API via the interactive /docs interface.

## Project Structure
data_api/
    ├── .gitignore
    ├── .env.example
    ├── README.md
    ├── main.py
    ├── pyproject.toml
    ├── poetry.lock
    ├── processor/
    │   ├── __init__.py
    │   ├── load_data.py
    │   ├── clean.py
    │   ├── aggregate.py
    └── tests/
        ├── __init__.py
        └── test_processor.py

## Features:
FastAPI Endpoint: A POST endpoint that loads, cleans, and aggregates dataset information.
Dependency Management: Use Poetry for package management.
Virtual Environment: Use venv for isolated Python environments.
Data Processing: Efficiently handle large datasets with Polars and Pandas.
Data Cleaning: Handle missing values, outliers, and incorrect formatting.
File Handling: Save processed results in JSON and Parquet formats.
API Documentation: Use Swagger UI (/docs) for interactive testing.

## Tasks

Task 1: Set Up Environment
Initialize a Python project using Poetry.
Set up a virtual environment (venv) and install FastAPI, Polars, Pandas.
Task 2: Data Loading
Create a script to load http://archive.ics.uci.edu/dataset/502/online+retail+ii.
Implement functions to read datasets using both Pandas and Polars.
Compare loading times and document findings by creating a benchmark function that returns;
data loading time
aggregation time.
Feel free to add any additional metrics you find interesting.

Task 3: Data Cleaning
Implement functions to handle missing values (e.g., fill with mean, drop rows).
Apply cleaning transformations using Pandas and Polars.
Document data cleaning process and reason behind your methodology.
Task 4: Data Aggregation
Group dataset by a categorical column (e.g., category).
Compute mean, sum, count for a numeric column.
Implement aggregation using both Pandas and Polars.
Expose results via a FastAPI endpoint (/aggregate).
Task 5: API Integration
Create a /process-data endpoint.
This endpoint should:
Load dataset
Clean and aggregate data
Return processed results in JSON format.
Implement error handling for invalid input formats.
Task 6: Saving and Retrieving Processed Data
Save processed data in JSON and Parquet formats.
Implement API endpoints to download these files (/download-json, /download-parquet).
Deadline
The project should be submitted by Friday, February 28th, 2025, 11:59 PM.
Submission
Share your PR in the task-submission channel and tag your mentors.
GitHub Repository: https://github.com/Data-Epic/data-wrangling/
Rubrics
Completeness: All parts of the task are implemented and working.
Code Quality: Code is clean, well-documented, and follows best practices.
Testing: Unit tests are comprehensive and cover edge cases.
Git Usage: Proper use of Git for version control, with meaningful commit messages.
Good luck, and happy coding! 🚀