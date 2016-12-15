import sys
import pandas as pd

# spark = SparkSession.builder.appName("yelp").getOrCreate()

# business = spark.read.json('/home/hadoop/yelp_dataset_challenge_academic_dataset/business_cleaned.json')
# business.registerTempTable("business")

# review = spark.read.json('/home/hadoop/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json')
# review.registerTempTable("review")


def review_count_by_date(df):
    df['date'] = pd.to_datetime(df['date'])
    df = (df
 .groupby('date')
     .stars
     .count()
 .resample('D')
     .sum()
 .fillna(0)
 
 )
    df = df.cumsum()
    df = df.ix['2015-01-01':]
    df = df.reset_index()
    df.columns = ['date','review_count']
    return df

def get_review_count_by_date(spark, business_id):
    df = spark.sql("""select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()
    review_count = review_count_by_date(df)
    return review_count.to_json()

def review_avg_by_date(df):
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['moving_avg'] = (df
                    .stars
            .ewm(span=28)
             .mean()
        )
    review_count = (df
                    .groupby('date')
                    .moving_avg
                    .last()
                    .resample('D')
                    .ffill()
    )
    review_count.index = review_count.index.map(lambda x: str(x.date()))
    review_count = review_count.ix['2015-01-01':].reset_index()
    review_count.columns = ['date','avg_rating']
    return review_count

def main(business_id):
	df = spark.sql("""select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()

	review_count = review_count_by_date(df)
	review_count.to_csv('./review_count.tsv', sep='\t', header=True, index=False)


	review_avg = review_avg_by_date(df)
	review_avg.to_csv('./review_avg.tsv', sep='\t', header=True, index=False)

	review_overlap = spark.sql("""select b.name, count(1) review, SUM(if(r1.stars >= 4,1,0)) as good_reviews, SUM(if(r1.stars <= 2,1,0)) as bad_reviews
		from review r1
		join review r2 on r1.user_id = r2.user_id and r1.business_id != r2.business_id
		join business b on b.business_id = r2.business_id
		where r1.business_id = '{business_id}'
		group by b.name""".format(business_id=business_id)).toPandas()

	review_overlap.to_csv('./review_count.csv', index=False, header=True)

if __name__ == '__main__':
	main(sys.argv[1])

# from pyspark import SparkConf, SparkContext

# from pyspark.sql import SQLContext


# sc = spark.sparkContext



# sc = SparkContext(cluster_url, job_name, pyFiles=pyFiles, conf=conf)

# sql_context = SQLContext(sc)



# events_sdf = sql_context.read.option("mergeSchema", "true").parquet(*uris)

# events_sdf.registerTempTable('events')