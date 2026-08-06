# dbtpyth_demo

Sample dbt project demonstrating a **Python model backed by a PySpark
DataFrame**, alongside ordinary SQL models, in a single DAG.

## DAG

```
raw_customers (seed) ─┐
                       ├─> stg_customers (SQL) ─┐
raw_orders (seed) ─────┼─> stg_orders (SQL) ────┴─> customer_order_summary (PYTHON / Spark DataFrame)
                                                              │
                                                              └─> high_value_customers (SQL)
```

- `seeds/` — sample CSVs (`raw_customers`, `raw_orders`) standing in for
  tables an EL tool would land in a raw schema.
- `models/staging/` — thin SQL models that select/clean from the
  declared `source()`.
- `models/marts/customer_order_summary.py` — a **Python model**. Instead
  of a `select`, it defines `model(dbt, session)`, pulls in
  `stg_customers` / `stg_orders` as Spark DataFrames via `dbt.ref(...)`,
  aggregates and transforms them with the PySpark DataFrame API
  (`groupBy`, `agg`, `join`, `withColumn`), and returns the resulting
  DataFrame for dbt to materialize as a table.
- `models/marts/high_value_customers.sql` — plain SQL reading the
  Python model's output via `ref()`, showing SQL and Python models
  composing freely in the same DAG.

## Requirements

- `dbt-core`
- `dbt-spark[session]` — the `session` connection method runs a local
  PySpark session in-process, so Python models can execute without a
  real Spark cluster or Databricks workspace.
- `pyspark`

```
pip install -r requirements.txt
```

## Running

```
export DBT_PROFILES_DIR=$(pwd)   # use the profiles.yml in this project
dbt seed
dbt run
dbt test
```

Note: dbt-core alone can parse this project, but only an adapter with
Python model support (dbt-spark, dbt-databricks, dbt-bigquery, or
dbt-snowflake) can actually *execute* `customer_order_summary.py`.

## Starting Spark Connect as a non-root user

Spark's `sbin/start-connect-server.sh` writes its PID and log files under
`$SPARK_HOME/logs` by default. If `$SPARK_HOME` (e.g. `/opt/spark`) is owned
by root or another user, a non-root user will hit permission errors on
startup. Point `SPARK_LOG_DIR` and `SPARK_PID_DIR` at a writable directory
instead:

```
mkdir -p ~/spark-logs ~/spark-pid
export SPARK_LOG_DIR=~/spark-logs
export SPARK_PID_DIR=~/spark-pid

$SPARK_HOME/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.2.0
```

The server listens on port `15002` by default.

To stop it, reuse the same `SPARK_PID_DIR` so the stop script can find the
PID file:

```
SPARK_PID_DIR=~/spark-pid $SPARK_HOME/sbin/stop-connect-server.sh
```

### Connecting and running Spark SQL

Spark Connect speaks gRPC, not JDBC/Thrift, so JDBC/ODBC clients like
`beeline` cannot connect to it — use a Spark Connect client instead:

> **`spark-sql --remote` does not work** on Spark 4.2.0. `bin/spark-sql`
> launches the Hive-CLI-based `SparkSQLCLIDriver`, which is not
> Spark-Connect-aware and always builds a classic local `SparkSession`
> regardless of `--remote`/`sc://...` — it ignores the flag and then fails
> with `A master URL must be set in your configuration`. Separately, Spark's
> launcher (`AbstractCommandBuilder`) strips the `spark-sql_`,
> `spark-sql-api_`, and `spark-connect_` jars from the classpath whenever
> `--remote` is passed, so even before that it fails with
> `NoClassDefFoundError: .../NonClosableMutableURLClassLoader`. Use
> `spark-connect-shell` or `pyspark` below instead.

**`spark-connect-shell`** (Scala REPL, purpose-built for Spark Connect):

```
$SPARK_HOME/bin/spark-connect-shell --remote sc://localhost:15002
```

```scala
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM some_table LIMIT 10").show()
```

**`pyspark` shell**:

```
$SPARK_HOME/bin/pyspark --remote sc://localhost:15002
```

```python
spark.sql("SHOW TABLES").show()
```

**Python script / notebook**, using the client library directly:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.sql("SELECT * FROM some_table LIMIT 10").show()
```
