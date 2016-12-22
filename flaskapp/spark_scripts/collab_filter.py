from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# read in Parquet file
review_parq = spark.read.parquet('yelp_data/reviews_collab.parquet')

# user ID hashing
tokenizer = Tokenizer(inputCol="user_id", outputCol="user_id_hash")
user_id_tokenizer = tokenizer.transform(review_parq)

hashingTF = HashingTF(inputCol="user_id_hash", outputCol="user_id_hash_res")
featurizedData = hashingTF.transform(user_id_tokenizer)

# business ID hashing
tokenizer = Tokenizer(inputCol="business_id", outputCol="business_id_hash")
business_id_tokenizer = tokenizer.transform(featurizedData)

hashingTF = HashingTF(inputCol="business_id_hash", outputCol="business_id_hash_res")
featurizedData = hashingTF.transform(business_id_tokenizer)

# todo: continue here

(training, test) = featurizedData.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
als = ALS(maxIter=5, regParam=0.01, userCol="user_id_hash_res", itemCol="business_id_hash_res", ratingCol="stars")

'''
>>> model = als.fit(training)
Traceback (most recent call last):
File "<stdin>", line 1, in <module>
File "/spark/python/pyspark/ml/base.py", line 64, in fit
return self._fit(dataset)
File "/spark/python/pyspark/ml/wrapper.py", line 213, in _fit
java_model = self._fit_java(dataset)
File "/spark/python/pyspark/ml/wrapper.py", line 210, in _fit_java
return self._java_obj.fit(dataset._jdf)
File "/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py", line 1133, in __call__
File "/spark/python/pyspark/sql/utils.py", line 79, in deco
raise IllegalArgumentException(s.split(': ', 1)[1], stackTrace)
pyspark.sql.utils.IllegalArgumentException: u'requirement failed: Column user_id_hash_res must be of type NumericType but was actually of type org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7.'
'''