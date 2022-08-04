# Processing movie_review.csv
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.ml.feature import Tokenizer, StopWordsRemover

# Create session
spark = SparkSession.builder.getOrCreate()

# Create a new data frame for movie_review.csv
movie_df = spark.read.csv('s3://s3-data-bootcamp-20220804183240579100000005/raw/movie_review.csv', sep=',', header=True)

# Take a little part of the data frame to make tests.
mini_df = movie_df.limit(100000)

# Use Tokenizer for converting "review_str" strings to a words array
tokenizer = Tokenizer(inputCol="review_str", outputCol="review_words")
tokenized = tokenizer.transform(mini_df).select("cid","id_review","review_words")

# Drop StopWords from review_words. The cleaned words are in "Cleaned"
cleaned = StopWordsRemover (inputCol = "review_words", outputCol = "Cleaned")
mini_df = cleaned.transform(tokenized).select("cid", "id_review", "Cleaned")

# Create a list of "good words" for describing a good movie
#good_words = ['good','nice','enjoyable','funny','interesting','surprising','hilarious','romantic','fast-moving','pleasant','fascinating','charming','best']
cool_df = mini_df.select("cid","id_review", array_contains(mini_df.Cleaned, "good").alias("positive_review"))

#output dataframe
super_df= cool_df.select(col("cid"), col("id_review"),col("positive_review").cast("integer"))

# Save df
super_df.write.mode("overwrite").parquet("s3://s3-data-bootcamp-20220804183240579100000005/output/")

spark.stop()
