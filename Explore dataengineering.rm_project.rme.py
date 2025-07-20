# Databricks notebook source
# MAGIC %md
# MAGIC #Improting Modules

# COMMAND ----------

# ==============================================
# CORE SPARK IMPORTS (TESTED)
# ==============================================
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext

# ==============================================
# DATAFRAME & SQL CORE (TESTED)
# ==============================================
from pyspark.sql import DataFrame, Row, Column
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.group import GroupedData
from pyspark.sql.catalog import Catalog
from pyspark.sql.observation import Observation
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F

# ==============================================
# FUNCTIONS & EXPRESSIONS (TESTED)
# ==============================================
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, 
    udf, array, create_map, struct,
    coalesce, isnull, isnan,
    expr, when
)

# Pandas UDFs (separate import for clarity)
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType

# ==============================================
# DATA PROCESSING UTILITIES (TESTED)
# ==============================================
from pyspark.sql import DataFrameNaFunctions
from pyspark.sql import DataFrameStatFunctions
from pyspark.sql.utils import (
    AnalysisException,
    ParseException,
    IllegalArgumentException
)

# ==============================================
# STREAMING IMPORTS (TESTED)
# ==============================================
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.streaming import StreamingQuery

# ==============================================
# TYPE SYSTEM (TESTED)
# ==============================================
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, 
    DoubleType, FloatType, DecimalType,
    BooleanType, BinaryType, DateType,
    TimestampType, ArrayType, MapType
)

# ==============================================
# UDF REGISTRATION (TESTED)
# ==============================================
from pyspark.sql.udf import UDFRegistration

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `dataengineering`.`rm_project`.`rme`;

# COMMAND ----------

# Access a table from the catalog
snack = spark.table('dataengineering.rm_project.snack_parquet')
rm = spark.table("dataengineering.rm_project.rme")
# Display the DataFrame
display(snack)
display(rm)

# COMMAND ----------

snack.select(col("Date"), col("EmployeeName"), col("SnackBrand")).collect()

# COMMAND ----------

data = [vars(snack), vars(rm)]
df = spark.createDataFrame(data)
display(df)

# COMMAND ----------

data = [vars(snack), vars(rm)]
rdd = spark.sparkContext.parallelize(data)
df = spark.createDataFrame(rdd)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop schema dataengineering.dataengineering cascade;
# MAGIC

# COMMAND ----------


