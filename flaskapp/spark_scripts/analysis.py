import sys
import pandas as pd

import yelp_lib

YELP_DATA_DIR = '/home/hadoop/yelp_data/'

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
    df.columns = ['date', 'review_count']
    return df


def get_review_count_by_date(business_id):
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    review.registerTempTable("review")
    df = spark.sql(
        """select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()
    review_count = review_count_by_date(df)
    return review_count.to_csv(None, sep='\t', header=True, index=False)


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
    review_count.columns = ['date', 'avg_rating']
    return review_count


def get_review_avg_by_date(business_id):
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    review.registerTempTable("review")
    df = spark.sql(
        """select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()
    review_count = review_avg_by_date(df)
    return review_count.to_csv(None, sep='\t', header=True, index=False)


def get_business_info(business_id):
    spark = yelp_lib.spark
    business = yelp_lib.get_parq('business')
    business.registerTempTable("business")
    df = spark.sql(
        """select * from business where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()
    return df

def get_top_words(business_id, n):
    word_freq = pd.read_json('{}wordfreq_bybusinessid_bigram.json'.format(YELP_DATA_DIR), orient='records')
    words = word_freq[word_freq['business_id'] == business_id].drop('business_id', axis=1).T
    words.columns = ['word_freq']
    words = words.sort_values('word_freq', ascending=False) # CHANGE TO SORT VALUES
    words = words.reset_index()
    words.columns = ['word','frequency']
    return words.head(n)

def main(business_id):
    df = spark.sql(
        """select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()

    review_count = review_count_by_date(df)
    review_count.to_csv('./review_count.tsv', sep='\t', header=True, index=False)

    review_avg = review_avg_by_date(df)
    review_avg.to_csv('./review_avg.tsv', sep='\t', header=True, index=False)

    review_overlap = spark.sql("""
        select b.name, count(1) review, SUM(if(r1.stars >= 4,1,0)) as good_reviews, SUM(if(r1.stars <= 2,1,0)) as bad_reviews
        from review r1
        join review r2 on r1.user_id = r2.user_id and r1.business_id != r2.business_id
        join business b on b.business_id = r2.business_id
        where r1.business_id = '{business_id}'
        group by b.name
    """.format(business_id=business_id)).toPandas()

    review_overlap.to_csv('./review_count.csv', index=False, header=True)


if __name__ == '__main__':
    main(sys.argv[1])
