# DuckDB Tutorial
This tutorial is composed of two exercises. In the first exercise, students will compare DuckDB,SQLite and Pandas in terms of performance and usability. The second exercise is about playing around with query execution and the query optimizer in DuckDB.

* The Exercise 2 Colab can be found [here](https://colab.research.google.com/drive/1HMtihjak75QBXfSOswsOyJyANnlH3Qmn#scrollTo=x4UIW8GUQaqt).

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

# **SETUP**

First we need to install DuckDB.
```python
!pip install duckdb --pre
```

# **Loading The Data**

We will work with a generated dataset from the TPC-H benchmark. DuckDB has built-in support for generating the dataset using the `dbgen` procedure.

We create an in-memory database and generate the data inside DuckDB using the following code snippet.

```python
import duckdb
con = duckdb.connect(':memory:')
con.execute("CALL dbgen(sf=0.1)")
```

# **Inspecting the Dataset**

The dataset consists of eight tables. We can see which tables are present in the database using the `SHOW TABLES` command.

Note that we append `.df()` to the query, this fetches the result as a Pandas DataFrame which renders nicely in Colab.

```python
con.execute("SHOW TABLES").df()
```

Using the `DESCRIBE` command, we can inspect the columns that are present in each of the tables. For example, we can inspect the lineitem table as follows:```

```python
con.execute("DESCRIBE lineitem").df()
```

We can use the `LIMIT` clause to inspect the first few rows of the lineitem table and display them.```

```python
con.execute("SELECT * FROM lineitem LIMIT 10").df()
```

To get a better feeling of what a table contains, we can use the `SUMMARIZE` command. This prints out several statistics about each of the columns of the table, such as the min and max value, how many unique values there are, the average value in the column, etc.```

```python
con.execute("SUMMARIZE lineitem").df()
```

# **Testing and Benchmarking**

Let us start our assignment by running a microbenchmark against the TPC-H dataset.

In order to make the benchmarking more interesting, let's compare against the SQLite database. This is a typical OLTP (transactional) database that is included along with every Python installation.

### **SQLite Setup**

We start out by creating a new in-memory database, just as we did in DuckDB.

```python
import sqlite3
sqlite_con = sqlite3.connect(':memory:', check_same_thread=False)
```

We can transfer the data from DuckDB to SQLite using a Pandas DataFrame as well. First, we export the data from DuckDB into a Pandas DataFrame using the `.df()` command. Then we use the `to_sql` function to write the data to our SQLite database.```

```python
table_list = con.execute("SHOW TABLES").fetchall()

for table in table_list:
  tname = table[0]
  table_data = con.table(tname).df()
  table_data.to_sql(tname, sqlite_con)
```

# **Running the Benchmark**

We have created a query down below which resembles a (simplified) query from the TPC-H benchmark:

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

```

Let's run the query in both SQLite and in DuckDB and measure the execution time.

```python

import time
import pandas as pd


def run_query(con_obj, q):
  start = time.time()
  con_obj.execute(q).fetchall()
  end = time.time()
  return(str(round(end - start,3)) + 's')

duckdb_results = [run_query(con, query)]
sqlite_results = [run_query(sqlite_con, query)]

pd.DataFrame.from_dict({
    'DuckDB': duckdb_results,
    'SQLite': sqlite_results
})

```

Using the `PRAGMA disable_optimizer` we can also disable the query optimizer of DuckDB, and re-run the query. In this manner we can see the performance effect that query optimization has on our query.```

```python
con.execute("PRAGMA disable_optimizer")
duckdb_unoptimized_results = [run_query(con, query)]
con.execute("PRAGMA enable_optimizer")

pd.DataFrame.from_dict({
    'DuckDB': duckdb_results,
    'DuckDB (Unoptimized)': duckdb_unoptimized_results,
    'SQLite': sqlite_results
})

```

# **Inspecting the Query Plan**
The query plan of a query can be inspected by prefixing the query with`EXPLAIN`. By default, only the physical query plan is returned. You can use `PRAGMA explain_output='all'` to output the unoptimized logical plan, the optimized logical plan and the physical plan as well.

```python
def explain_query(query):
  print(con.execute("EXPLAIN " + query).fetchall()[0][1])

explain_query(query)

```

# **Profiling Queries**
Rather than only viewing the query plan, we can also run the query and look at the profile output. The function `run_and_profile_query` below performs this profiling by enabling the profiling, writing the profiling output to a file, and then printing the contents of that file to the console.

The profiler output shows extra information for every operator; namely how much time was spent executing that operator, and how many tuples have moved from that operator to the operator above it. 

For a `SEQ_SCAN` (sequential scan), for example, it shows how many tuples have been read from the base table. For a `FILTER`, it shows how many tuples have passed the filter predicate. For a``HASH_GROUP_BY`, it shows how many groups were created and aggregated.

These intermediate cardinalities are important because they do a good job of explaining why an operator takes a certain amount of time, and in many cases these intermediates can be avoided or drastically reduced by modifying the way in which a query is executed.


```python
def run_and_profile_query(query):
  con.execute("PRAGMA enable_profiling")
  con.execute("PRAGMA profiling_output='out.log'")
  con.execute(query)
  con.execute("PRAGMA disable_profiling")
  with open('out.log', 'r') as f:
    output = f.read()
  print(output)
  
run_and_profile_query(query)
```

# **Query Optimizations**

An important component of a database system is the optimizer. The optimizer changes the query plan so that it is logically equivalent to the original plan, but (hopefully) executes much faster.

In an ideal world, the optimizer allows the user not to worry about how to formulate a query: the user only needs to describe what result they want to see, and the database figures out the most efficient way of retrieving that result.

In practice, this is certainly not always true, and in some situations it is necessary to rephrase a query. Nevertheless, optimizers generally do a very good job at optimizing queries, and save users a lot of time in manually reformulating queries.

Let us run the following query and see how it performs:

```python
unoptimized_query = """
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

run_and_profile_query(unoptimized_query)

```

# **Manual Query Optimizations**

In order to get a better idea of how query optimizers work, we are going to perform *manual* query optimization. In order to do that, we will disable all query optimizers in DuckDB, which means the query will run *as-is*. We can then change the way the query is physically executed by altering the query. Let's try to disable the optimizer and looking at the query plan:

```python
# con.execute("PRAGMA disable_optimizer")
explain_query(unoptimized_query)
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

# **Assignment**

Now the query actually finishes; however, it is still much slower than before. There are more changes that can be made to the query to make it run faster. Your assignment (and challenge!) is to adjust the query so that it runs in similar speed to the query with optimizations enabled. You will be the human query optimizer replacing the disabled one.

Hint:

1. Join order matters!
2. DuckDB always builds the hash table on the *right side* of a hash join.
3. Filters? Projections?

Another important consideration is that the query optimization should still output the same query result! An optimizer that changes the query result is incorrect and is considered a bug.

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

# **Bonus Assignment 1**

The TPC-H queries can be loaded from DuckDB using the query `SELECT * FROM tpch_queries()`. Run all the queries in both DuckDB and SQLite and compare the results.

Note: Not all queries will work as-is in SQLite, and some might need to be (slightly) rewritten to accomodate SQLite's (more limited) SQL dialect.

# **Bonus Assignment 2**

As a bonus assignment, here is another query that you can optimize.

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


run_and_profile_query(query)
```
