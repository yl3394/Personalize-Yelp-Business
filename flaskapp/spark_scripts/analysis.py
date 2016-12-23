import sys
import pandas as pd
import json
import yelp_lib
from pyspark.sql.functions import explode
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import NaiveBayes
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
import numpy as np
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


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
    df.index = df.index.map(lambda x: str(x.date()))
    df = df.reset_index()
    df.columns = ['date', 'review_count']
    return df


def get_review_count_by_date(business_id):
    """
    Returns a cumulative count of reviews by date.
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    review.registerTempTable("review")
    df = spark.sql(
        """select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()
    review_count = review_count_by_date(df)
    return json.dumps({'date':review_count['date'].tolist(), 
                        'review_count': review_count['review_count'].tolist()})

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
    """
    Returns a smoothed review average grouped by date.
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    review.registerTempTable("review")
    df = spark.sql(
        """select * from review where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()
    review_avg = review_avg_by_date(df)
    return json.dumps({'date':review_avg['date'].tolist(), 
                        'avg_rating': review_avg['avg_rating'].tolist()})


def get_business_info(business_id):
    """
    Returns fields from the business table along 
    with total checkins for the given business.
    """
    spark = yelp_lib.spark
    business = yelp_lib.get_parq('business')
    business.registerTempTable("business")
    df = spark.sql(
        """select * from business where business_id = '{business_id}'""".format(business_id=business_id)).toPandas()

    checkin_file = '{}total_checkins.csv'.format(YELP_DATA_DIR)
    checkins = pd.read_csv(checkin_file)
    checkins = checkins[checkins['business_id'] == business_id]

    try:
        df['checkins'] = checkins['total_checkins'].values[0]
    except:
        df['checkins'] = 0
    
    return df

def get_checkins(business_id):
    """
    Returns the distribution of checkins by day and hour
    """
    checkin_file = '{}yelp_academic_dataset_checkin.json'.format(YELP_DATA_DIR)
    checkin_df = pd.read_json(checkin_file, orient='records', lines=True)
    info = checkin_df.loc[lambda x: x['business_id'] == business_id, 'checkin_info']

    checkins = pd.DataFrame.from_dict(info.values[0], orient='index')
    checkins = checkins.reset_index()
    checkins.columns = ['time', 'checkins']
    checkins['hour'] = checkins['time'].str.split('-').str.get(0)
    checkins['day'] = checkins['time'].str.split('-').str.get(1)

    checkins_by_day = (checkins
                       .groupby('day')
                       .sum()
                       .reindex(index=['0', '1', '2', '3', '4', '5', '6'])
                       .fillna(0)
                       )
    checkins_by_day.index = ['Sun', 'Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat']
    checkins_by_day = checkins_by_day.reset_index()
    checkins_by_day.columns = ['day', 'checkins']

    checkins_by_hour = (checkins
                       .groupby('hour')
                       .sum()
                       )
    checkins_by_hour.index = checkins_by_hour.index.astype('int')
    checkins_by_hour = (checkins_by_hour
     .reindex(index=range(24))
     .fillna(0)
     .reset_index()
     )

    return json.dumps({'day': {'day':checkins_by_day['day'].tolist(), 
                        'checkins': checkins_by_day['checkins'].tolist()}, 
                      'hour': {'hour':checkins_by_hour['hour'].tolist(), 
                        'checkins': checkins_by_hour['checkins'].tolist()}, 
                        })


def get_reviews(business_id, n):
    """
    Returns a sample of top n reviews sorted by 'usefulness'
    for the given business.
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    business_df = review.filter(review['business_id'] == business_id).toPandas()
    business_df['usefulness'] = business_df['votes'].apply(lambda x: x[1])
    business_df = business_df.sort_values('usefulness', ascending=False)
    return business_df.head(n).to_dict(orient='records')

def get_top_words(business_id, n, kind='all'):
    """
    Returns the most frequent tokens and their associated frequencies.
    Performes tokenization and removal of stopwords.
    Normalized frequencies for use in word clouds.

    Input:
    - kind: ['all', 'good', 'bad']
        Generate output for all reviews, or only good or bad.
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    business_df = review.filter(review['business_id'] == business_id)

    if kind == 'good':
        business_df = business_df.filter(business_df['stars'] >= 4)
    elif kind == 'bad':
        business_df = business_df.filter(business_df['stars'] < 3)

    regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    wordsDataFrame = regexTokenizer.transform(business_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    cleaned = remover.transform(wordsDataFrame)

    all_words = cleaned.select(explode('filtered'))
    word_counts = all_words.rdd.countByValue()
    word_count_df = pd.DataFrame.from_dict(word_counts, orient='index')
    word_count_df.index = map(lambda x: x[0], word_count_df.index)
    word_count_df.columns = ['word_freq']
    word_count_df = word_count_df.sort_values('word_freq', ascending=False) 
    word_count_df = word_count_df.reset_index()
    word_count_df.columns = ['word','frequency']
    word_count_df['frequency'] = word_count_df['frequency'] / word_count_df['frequency'].max() * 60.
    return word_count_df.head(n).to_json(orient='records')


def bayes(business_id):
    """
    Generates naive bayes model of rating based on tokens from the review text.
    Model generates "thetas" that correspond to P('good' or 'bad' | presence of token)
    Returns tokens that correspond to 'good' and 'bad' reviews based on the difference in probabilities.
    Normalizes for use in word clouds.
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    business_df = review.filter(review['business_id'] == business_id)

    regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    wordsDataFrame = regexTokenizer.transform(business_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    cleaned = remover.transform(wordsDataFrame)
    
    star_mapping = {0: 0.0,
                1: 0.0,
                2: 0.0,
                3: 0.0,
                4: 1.0,
                5: 1.0}

    cleaned = cleaned.replace(star_mapping, 'stars')
    cleaned = cleaned.withColumn("stars", cleaned["stars"].cast("double"))

    cv = CountVectorizer(inputCol="filtered", outputCol="features")
    model = cv.fit(cleaned)
    vectorized = model.transform(cleaned)

    vectorized = vectorized.select(col('stars').alias('label'), col('features'))

    # create the trainer and set its parameters
    nb = NaiveBayes(smoothing=1.0)
    # train the model
    nb_model = nb.fit(vectorized)

    thetas = nb_model.theta

    thetas = thetas.toArray()
    result_df = pd.DataFrame({'word': model.vocabulary,
                  'theta0': thetas[0],
                  'theta1': thetas[1]})

    result_df['t0p'] = np.exp(result_df['theta0'])
    result_df['t1p'] = np.exp(result_df['theta1'])

    result_df['diff'] = result_df['t1p'] / result_df['t0p']

    word_list = result_df.sort_values('diff', ascending=False)[['word', 'diff']]
    word_list.columns = ['word', 'frequency']
    good = word_list.head(30)
    good['frequency'] = word_list['frequency'] / word_list['frequency'].max() * 60.
    
    bad = word_list.tail(30)
    bad['frequency'] = 1.0 / bad['frequency']
    bad['frequency'] = bad['frequency'] / bad['frequency'].max() * 60.
    return json.dumps({'good': good.to_dict(orient='records'),
                      'bad': bad.to_dict(orient='records')})


def get_review_overlap(business_id, n):
    """
    Returns a list of the top N businesses that most overlap with the given restaurant.
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    business = yelp_lib.get_parq('business')
    review.registerTempTable('review')
    business.registerTempTable('business')
    review_overlap = spark.sql("""
    select b.name, count(1) review, SUM(if(r1.stars >= 4,1,0)) as good_reviews, SUM(if(r1.stars <= 2,1,0)) as bad_reviews
    from review r1
    join review r2 on r1.user_id = r2.user_id and r1.business_id != r2.business_id
    join business b on b.business_id = r2.business_id
    where r1.business_id = '{business_id}'
    group by b.name
    """.format(business_id=business_id)).toPandas()
    review_overlap = review_overlap.sort_values('review', ascending=False)
    review_overlap = review_overlap.head(n)
    return json.dumps({'name':review_overlap['name'].tolist(), 
                        'good_reviews': review_overlap['good_reviews'].tolist(),
                        'bad_reviews': review_overlap['bad_reviews'].tolist()})


def bayes_cv(business_id):
    """
    Crossvalidation of bayes model
    """
    spark = yelp_lib.spark
    review = yelp_lib.get_parq('review')
    business_df = review.filter(review['business_id'] == business_id)

    regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    wordsDataFrame = regexTokenizer.transform(business_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    cleaned = remover.transform(wordsDataFrame)
    
    star_mapping = {0: 0.0,
                1: 0.0,
                2: 0.0,
                3: 0.0,
                4: 1.0,
                5: 1.0}

    cleaned = cleaned.replace(star_mapping, 'stars')
    cleaned = cleaned.withColumn("stars", cleaned["stars"].cast("double"))

    cv = CountVectorizer(inputCol="filtered", outputCol="features")
    model = cv.fit(cleaned)
    vectorized = model.transform(cleaned)

    vectorized = vectorized.select(col('stars').alias('label'), col('features'))

    splits = vectorized.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]

    # create the trainer and set its parameters
    nb = NaiveBayes(smoothing=1.0)
    # train the model
    nb_model = nb.fit(train)
    # compute accuracy on the test set
    result = nb_model.transform(test)

    predictionAndLabels = result.select("prediction", "label")
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    return "Accuracy: " + str(evaluator.evaluate(predictionAndLabels))
