from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("yelp").getOrCreate()
parqs = {}


def get_parq(name):
    """
    Caches parquet file on the first lookup. Returns cached object on subsequent lookups.
    :param name: parquet file to load, e.g. 'business', 'review'
    :return: parquet file
    """
    if name not in parqs:
        parqs[name] = spark.read.load('/home/hadoop/yelp_data/yelp_{}.parquet'.format(name))
    return parqs[name]


def get_business_names(name):
    """
    Prints all business names contanining 'name'.
    """
    if not name:
        return ""

    business_parq = get_parq('business')

    sel = business_parq.select(
        'name', 'business_id', 'city', 'state', 'stars', 'review_count', 'categories'
    ).where(
        'LOWER(name) like "%{}%"'.format(name.lower())
    ).orderBy('name')
    coll = sel.collect()

    # names may contain non-ASCII chars - encode
    res = ''.join(
        '{};{};{};{};{};{};{}|'.format(
            r.name.encode('utf8'),
            r.business_id,
            r.city.encode('utf8'),
            r.state.encode('utf8'),
            r.stars,
            r.review_count,
            ''.join('"{}" '.format(c) for c in r.categories if c != 'Restaurants')
        ) for r in coll
    )
    # remove trailing separator
    res = res[:-1]

    return res
