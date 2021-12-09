import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import when

sc = SparkContext()
spark = SparkSession(sc)

movie_reviewdf = spark.read.csv("gs://de-bootcamp-gcs-staging/user_purchase", header=True, inferSchema=True)
class_moviewreviewdf = spark.read.option("header", "true").parquet("gs://de-bootcamp-gcs-staging/classificationrevlogic/part-00000-6b24ba42-d635-4611-a7b1-d7b18d0b1cfe-c000.snappy.parquet")
