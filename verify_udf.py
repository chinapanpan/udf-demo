"""
Demo: Create permanent UDF via JAR and verify in Spark SQL
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UDF Demo - Create and Verify") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 60)
print("Spark version:", spark.version)
print("=" * 60)

# Step 1: Create database
print("\n[Step 1] Create database")
spark.sql("CREATE DATABASE IF NOT EXISTS udf_demo")
spark.sql("USE udf_demo")
print("Database udf_demo ready")

# Step 2: Create permanent UDF
print("\n[Step 2] Create permanent UDF via CREATE FUNCTION")
spark.sql("DROP FUNCTION IF EXISTS udf_demo.to_upper")
spark.sql("""
    CREATE FUNCTION udf_demo.to_upper
    AS 'com.example.udf.ToUpperUDF'
    USING JAR 's3://sagemaker-us-west-2-340636688520/udf-demo/udf-demo-1.0.0.jar'
""")
print("Function udf_demo.to_upper created!")

# Step 3: Show functions
print("\n[Step 3] Show registered functions")
spark.sql("SHOW USER FUNCTIONS IN udf_demo").show(truncate=False)

# Step 4: Verify with simple query
print("\n[Step 4] Verify UDF with simple query")
spark.sql("SELECT udf_demo.to_upper('hello world') AS result").show(truncate=False)

# Step 5: Verify with table data
print("\n[Step 5] Verify UDF with table data")
spark.sql("DROP TABLE IF EXISTS udf_demo.test_names")
spark.sql("""
    CREATE TABLE udf_demo.test_names (name STRING, city STRING)
    USING parquet
""")
spark.sql("""
    INSERT INTO udf_demo.test_names VALUES
    ('alice', 'seattle'),
    ('bob', 'portland'),
    ('charlie', 'san francisco')
""")

spark.sql("""
    SELECT
        name,
        udf_demo.to_upper(name) AS upper_name,
        city,
        udf_demo.to_upper(city) AS upper_city
    FROM udf_demo.test_names
""").show(truncate=False)

# Step 6: Verify with Spark SQL built-in functions side by side
print("\n[Step 6] Compare with built-in upper()")
spark.sql("""
    SELECT
        name,
        upper(name) AS builtin_upper,
        udf_demo.to_upper(name) AS custom_udf_upper
    FROM udf_demo.test_names
""").show(truncate=False)

# Cleanup
print("\n[Cleanup]")
spark.sql("DROP TABLE IF EXISTS udf_demo.test_names")
spark.sql("DROP FUNCTION IF EXISTS udf_demo.to_upper")
spark.sql("DROP DATABASE IF EXISTS udf_demo")
print("Cleanup done")

spark.stop()
print("\nDone!")
