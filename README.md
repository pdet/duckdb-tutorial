# DuckDB Tutorial for SBBD
This tutorial is composed of two exercises. In the first exercise, students will compare DuckDB,SQLite and Pandas in terms of performance and usability. The second exercise is about implementing a User Defined Function (UDF) inside DuckDB.

## (1) Using DuckDB.

### Requirements
For this exercise, you will need a [Google Colab Account](https://colab.research.google.com/).

### Task
Download the .ipynb file from [here](https://github.com/pdet/duckdb-tutorial/DuckDB_Exercise1.ipynb) and upload it as a Python 3 Notebook into [Google Colab](https://colab.research.google.com/).
Follow the steps depicted in the python notebook, and compare the performance of these 3 engines on three different tasks. You will load the data, execute different queries (focusing in selections, aggregations and joins) and finally will perform transactions cleaning dirty tuples from our dataset.

### Project Assignment
Similar to the task described above, you must download the .ipynb file from [here](https://github.com/pdet/duckdb-tutorial/blob/master/NYC_Cab_DuckDB_Assignment.ipynb) and upload it as a Python 3 Notebook into [Google Colab](https://colab.research.google.com/). In this assignment, you will experiment with the NYC Cab dataset from 2016. This dataset provides information (e.g., pickup/dropoff time, # of passengers, trip distance, fare) about cab trips done in New York City during 2016. You can learn more about the dataset clicking [here!](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

You will load this dataset into pandas, sqlite, and duckdb. You will compare the performance of multiple data-science like queries, including performing a fare estimation (i.e., predicting how much a ride will cost depending on distance) using machine learning.

In the first section you will implement the loader in duckdb **[5 points].**

The second section has two data-science like queries, the implementation in pandas is already given, and you should use it as a logical/correctness reference to write the queries for sqlite and duckdb, remember to compare the performance of the three different systems **[25 points]**.

Finally, in the third section you will implement a simple machine learning algorithm in duckdb to predict fare costs. A full implementation of pandas is given and a partial of sqlite. Again, use them as a logical/correctness reference and compare the performance of the three different systems. **[40 points]**

Remember to submit your notebook with the answers to all sections as well as a PDF document (max two papes) listing all experienced execution times and reasoning about the performance difference in these systems.

## (2) Implementing Scalar Functions in DuckDB.
In this assignment, we will implement our own scalar function in DuckDB.

### Requirements
DuckDB requires [CMake](https://cmake.org) to be installed and a `C++11` compliant compiler. GCC 4.9 and newer, Clang 3.9 and newer and VisualStudio 2017 are tested on each revision, but any `C++11` compliant compiler should work.

### Building
The source code can be downloaded from [the DuckDB repository](https://github.com/cwida/duckdb/commits/master). Run the following command to clone the github directory:
```bash
git clone https://github.com/cwida/duckdb
```

Alternatively, a zip file with the source code can be downloaded from [here](https://github.com/cwida/duckdb/archive/master.zip).

After downloading the source code, the build must be initialized with CMake and the source files must be built. On Linux/OSX, the build can be done by simply using the command `make debug`. On Windows, you must use CMake to generate a Visual Studio project and then use Visual Studio to build that project. Building might take some time depending on how fast your computer is!

If you build using the `make debug` command, the build will be placed in the `build/debug` directory. A shell can be found in the location `build/debug/tools/shell/shell` from which you can issue simple commands to DuckDB. This shell is based on the `sqlite3` shell.

### Creating a Simple Function
For the example, we will create the following simple function: ```add_one(INTEGER) -> INTEGER```. This function adds one to its input value and returns the result. 

##### Creating the Tests
It is easiest to start developing by first creating tests, this allows us to already model the correct behavior and to later verify that our function achieves the correct behavior. The tests for functions are located in the `test/sql/function` directory. Navigate there, and create the file `test_add_one.cpp` to test our function. Then open the `CMakeLists.txt` in that same directory (`test/sql/function/CMakeLists.txt`) and add the file `test_add_one.cpp` to the to-be-built files.

In the test file, we can add the following snippet of code to test our function:
```cpp
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test add one function", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// 1 + 1 = 2
	result = con.Query("SELECT add_one(1)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// NULL + 1 = NULL
	result = con.Query("SELECT add_one(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	// NULL, 1, 2, 3 -> NULL, 2, 3, 4
	result = con.Query("SELECT add_one(i) FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 4}));
	// 2, 3 -> 3, 4
	result = con.Query("SELECT add_one(i) FROM integers WHERE i > 1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4}));
}
```

After rebuilding, we can run our test by running the `unittest` program: `build/debug/test/unittest "Test add one function"`. On Windows, we can run it by running the `unittest` program and adding `"Test add one function"` as the command line parameters. For now though, our function still does not exist and hence our tests fail with the following error message:

`Catalog: Function with name add_one does not exist!`

##### Implementation
Now that we have created our test, we can add our implementation. The implementation for functions lives in the `src/function/scalar` directory. In this case, we will place our function in the `math` subdirectory. Create the file `src/function/scalar/math/add_one.cpp` and add the following body of code:

```cpp
#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void add_one_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	// initialize the result
	result.Initialize(TypeId::INTEGER);
	// now loop over the input vector
	VectorOperations::UnaryExec<int32_t, int32_t>(
		inputs[0], result, [&](int32_t input) {
		return input + 1;
	});
}

void AddOne::RegisterFunction(BuiltinFunctions &set) {
  // register the function
	set.AddFunction(ScalarFunction("add_one", { SQLType::INTEGER }, SQLType::INTEGER, add_one_function));
}
```

Also add the file `add_one.cpp` to `src/function/scalar/math/CMakeLists.txt`.

To complete our function definition, we need to add a few more lines of boilerplate. First, in the file `src/function/scalar/math_functions.cpp` add the following snippet of code in the `RegisterMathFunctions` function:

```cpp
Register<AddOne>();
```

Finally, in the file `src/include/duckdb/function/scalar/math_functions.hpp`, add the following snippet of code:
```cpp
struct AddOne {
	static void RegisterFunction(BuiltinFunctions &set);
};
```

After that our function should work! We can now test our function from within the shell our again by running our unittest program and verifying that it provides the correct result in all cases.

##### Assignment: Creating your own function
A list of functions from other systems that DuckDB is currently lacking can be found [here](https://github.com/cwida/duckdb/issues/193). If you implement the functions successfully, feel free to submit a pull request! 

The following functions are good starting points:

`RTRIM(VARCHAR) -> VARCHAR             [ MySQL  ]`
Remove spaces on right side of string

`REVERSE(VARCHAR) -> VARCHAR           [ MySQL  ]`
Reverse a string (mind the unicode!)

`REPEAT(VARCHAR, INTEGER) -> VARCHAR   [ MySQL  ]`
Repeat the specified string a number of times

`INSTR(VARCHAR, VARCHAR) -> BOOL       [ SQLite ]`
Returns true if second string is part of first string

Note that for functions that return a string, the string should be added to the string_heap inside the function by calling the `result.string_heap.AddString()` function prior to returning. For an example of this, see e.g. `src/function/scalar/string/substring.cpp:59`.
