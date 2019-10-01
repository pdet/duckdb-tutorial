# DuckDB Tutorial for SBBD
This tutorial is composed of two exercises. In the first exercise, students will  integrate a ML pipeline into DuckDB and compare its performance with SQLite. The ML model's goal is to perform classification of voters in North Carolina. Predicting which party voters will vote for in the upcoming election based on the previous election results. The second exercise is about implementing a User Defined Function (UDF) inside DuckDB.

## (1) Voter Classification - ML Pipeline Integration.

### Requirements
For this exercise, you will need [python3](https://www.python.org/) and [pip](https://pypi.org/project/pip/) installed in your machine. All other dependencies and data will be automatically installed with the scripts.

### Files
Withing folder 'exercise_1' you will find the following scripts:
* setup.py -> It downloads all dependencies, the data that will be used in this exercise and decompresses it.
* sqlite_load.py -> It loads the data (as is) into a 'voters_sqlite.db' sqlite instance.
* sqlite_benchmark.py -> It performs the full ML Pipeline, and prints out the time spent on the database queries.
* duckdb_load.py -> Is a skeleton for the duckdb loader you will be implementing.
* duckdb_benchmark.py -> Is a skeleton for the duckdb loader you will be implementing.

### ML Pipeline
(1) Preprocessing
All the data is loaded 

### Your goal
Your goal is to perform the same ML task but now using duckdb. Use the duckdb_benchmark.py as a guide, the script is a skeleton to help you in implementing this integration. Check the comments for hints ;-)

To Download the datasets and execute the pipeline in sqlserver you must execute the setup.py and the sqlite_benchmark.py scripts:
```bash
python3 setup.py
python3 sqlite_benchmark.py
```

## (2) UDF in DuckDB.
### Requirements
DuckDB requires [CMake](https://cmake.org) to be installed and a `C++11` compliant compiler. GCC 4.9 and newer, Clang 3.9 and newer and VisualStudio 2017 are tested on each revision.
