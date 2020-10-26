# DuckDB Tutorial
This tutorial is composed of two exercises. In the first exercise, students will compare DuckDB,SQLite and Pandas in terms of performance and usability. The second exercise is about playing around with query execution and the query optimizer in DuckDB.

### Requirements
For these exercise, you will need a [Google Colab Account](https://colab.research.google.com/).

## Part (1) Using DuckDB

### Task
Download the .ipynb file from [here](https://raw.githubusercontent.com/pdet/duckdb-tutorial/master/Part%201/Exercise/Exercise.ipynb) and upload it as a Python 3 Notebook into [Google Colab](https://colab.research.google.com/).
Follow the steps depicted in the python notebook, and compare the performance of these 3 engines on three different tasks. You will load the data, execute different queries (focusing in selections, aggregations and joins) and finally will perform transactions cleaning dirty tuples from our dataset.

### Project Assignment
Similar to the task described above, you must download the .ipynb file from [here](https://raw.githubusercontent.com/pdet/duckdb-tutorial/master/Project/NYC_Cab_DuckDB_Assignment.ipynb) and upload it as a Python 3 Notebook into [Google Colab](https://colab.research.google.com/). In this assignment, you will experiment with the NYC Cab dataset from 2016. This dataset provides information (e.g., pickup/dropoff time, # of passengers, trip distance, fare) about cab trips done in New York City during 2016. You can learn more about the dataset clicking [here!](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

You will load this dataset into pandas, sqlite, and duckdb. You will compare the performance of multiple data-science like queries, including performing a fare estimation (i.e., predicting how much a ride will cost depending on distance) using machine learning.

In the first section you will implement the loader in duckdb **[5 points].**

The second section has two data-science like queries, the implementation in pandas is already given, and you should use it as a logical/correctness reference to write the queries for sqlite and duckdb, remember to compare the performance of the three different systems **[25 points]**.

Finally, in the third section you will implement a simple machine learning algorithm in duckdb to predict fare costs. A full implementation of pandas is given and a partial of sqlite. Again, use them as a logical/correctness reference and compare the performance of the three different systems. **[40 points]**

Remember to submit your notebook with the answers to all sections as well as a PDF document (max two papes) listing all experienced execution times and reasoning about the performance difference in these systems.


## Part (2) Query Optimization

### Task
Download the .ipynb file from [here](https://raw.githubusercontent.com/pdet/duckdb-tutorial/master/Part%202/DuckDB_Exercise2.ipynb) and upload it as a Python 3 Notebook into [Google Colab](https://colab.research.google.com/). Follow the instructions in the notebook, and try to find a way to formulate the SQL query so that the query execution matches, or even exceeds the performance in DuckDB by serving as the manual query optimizer!

### Local Execution
For those who would rather run the tutorial locally, below are the contents of the IPython notebook repeated:

#### Setup
First we need to install DuckDB:
```bash
pip install duckdb --pre
```

#### Loading The Data
```python
import urllib.request
import zipfile

print("Downloading datasets")

urllib.request.urlretrieve("https://github.com/Mytherin/datasets/raw/main/tpch_sf01.zip", "tpch_sf01.zip")

print("Decompressing files")

with zipfile.ZipFile("tpch_sf01.zip","r") as zip_ref:
	zip_ref.extractall("./")

print("Finished.")
```

#### Load Data in DuckDB
```python
import duckdb
con = duckdb.connect(':memory:')

queries = []
with open('tpch_sf01/schema.sql', 'r') as f:
  queries += [x for x in f.read().split(';') if len(x.strip()) > 0]
with open('tpch_sf01/load.sql', 'r') as f:
  queries += [x for x in f.read().split(';') if len(x.strip()) > 0]

print("Beginning data load")
for q in queries:
  con.execute(q)
print("Finishing data load")
```

#### Inspecting the Query Plan
The query plan of a query can be inspected by prefixing the query with`explain`. By default, only the physical query plan is returned. You can use `PRAGMA explain_output='all'` to output the unoptimized logical plan, the optimized logical plan and the physical plan instead

```python
def explain_query(query):
  print(con.execute("EXPLAIN " + query).fetchall()[0][1])

query = """
SELECT l_orderkey, SUM(l_extendedprice)
FROM lineitem
WHERE l_discount < 5
GROUP BY l_orderkey
ORDER BY l_orderkey DESC;
"""

explain_query(query)
```

#### Profiling Queries
Rather than only viewing the query plan, we can also run the query and look at the profile output. The function `run_and_profile_query` below performs this profiling by enabling the profiling, writing the profiling output to a file, and then printing the contents of that file to the console.

The profiler output shows extra information for every operator; namely how much time was spent executing that operator, and how many tuples have moved from that operator to the operator above it.

For a `SEQ_SCAN` (sequential scan), for example, it shows how many tuples have been read from the base table. For a `FILTER`, it shows how many tuples have passed the filter predicate. For a `HASH_GROUP_BY`, it shows how many groups were created and aggregated.

These intermediate cardinalities are important because they do a good job of explaining why an operator takes a certain amount of time, and in many cases these intermediates can be avoided or drastically reduced by modifying the way in which a query is executed.



```python
def run_and_profile_query(query):
  con.execute("PRAGMA enable_profiling")
  con.execute("PRAGMA profiling_output='out.log'")
  con.execute(query)
  with open('out.log', 'r') as f:
    output = f.read()
  con.execute("PRAGMA disable_profiling")
  print(output)

query = """
SELECT l_orderkey, SUM(l_extendedprice)
FROM lineitem
WHERE l_discount < 5
GROUP BY l_orderkey
ORDER BY l_orderkey DESC;
"""

run_and_profile_query(query)
```

#### Query Optimizations
An important component of a database system is the optimizer. The optimizer changes the query plan so that it is logically equivalent to the original plan, but (hopefully) executes much faster.
In an ideal world, the optimizer allows the user not to worry about how to formulate a query: the user only needs to describe what result they want to see, and the database figures out the most efficient way of retrieving that result.
In practice, this is certainly not always true, and in some situations it is necessary to rephrase a query. Nevertheless, optimizers generally do a very good job at optimizing queries, and save users a lot of time in manually reformulating queries.
Let us run the following query and see how it performs:

```python
query = """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
"""

run_and_profile_query(query)
```

#### Manual Query Optimizations


In order to get a better idea of how query optimizers work, we are going to perform *manual* query optimization. In order to do that, we will disable all query optimizers in DuckDB, which means the query will run *as-is*. We can then change the way the query is physically executed by altering the query. Let's try to disable the optimizer and looking at the query plan:

```python
con.execute("PRAGMA disable_optimizer")
explain_query(query)
```

Looking at the plan you now see that the hash joins that were used before are replaced by cross products followed by a filter. This is what was literally written in the query, however, cross products are extremely expensive! We could run this query, but because of the cross products it will take extremely long.

Let's rewrite the query to explicitly use joins instead, and then we can actually run it:

```python
query = """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer
    JOIN orders ON (c_custkey=o_custkey)
    JOIN lineitem ON (l_orderkey=o_orderkey)
WHERE
    c_mktsegment = 'BUILDING'
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
"""

run_and_profile_query(query)

```

##### Assignment

Now the query actually finishes; however, it is still much slower than before. There are more changes that can be made to the query to make it run faster. Your assignment (and challenge!) is to adjust the query so that it runs in similar speed to the query with optimizations enabled. You will be the human query optimizer replacing the disabled one.

Hint:

1. Join order matters!
2. DuckDB always builds the hash table on the *right side* of a hash join.
3. Filters? Projections?

```python
query = """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer
    JOIN orders ON (c_custkey=o_custkey)
    JOIN lineitem ON (l_orderkey=o_orderkey)
WHERE
    c_mktsegment = 'BUILDING'
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
"""

run_and_profile_query(query)
```

##### Bonus Assignment
As a bonus assignment, here is another query that you can optimize. Note that this query is currently NOT fully optimized by DuckDB because of a problem in the query optimizer, hence on this query it is actually possible to not only match, but beat the DuckDB query optimizer!

```python
query = """
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
"""


# HINT: first replace the cross products with joins before running this query!
# run_and_profile_query(query)
```