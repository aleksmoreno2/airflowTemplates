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

movie_reviewdf = spark.read.csv("gs://de-bootcamp-gcs-raw/movie_review.csv", header=True, inferSchema=True)

tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")

countTokens = udf(lambda words: len(words), IntegerType())
tokenized = tokenizer.transform(movie_reviewdf)
tokenized.select("review_str", "review_token").withColumn("tokens", countTokens(col("review_token"))).show(5)

remover = StopWordsRemover(inputCol="review_token", outputCol="filtered")
newdf = remover.transform(tokenized)

newdf = newdf.withColumn("positive_review", F.when(F.array_contains("filtered", "good"), '1').otherwise('0'))

newdf.select("cid", "positive_review").write.format("parquet").mode('overwrite').save('gs://de-bootcamp-gcs-staging'+'/'+'/classificationrevlogic')