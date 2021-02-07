import pyspark
import json


def main():

    # Task 1
    review_path = '/Users/yijunlin/Github/cis-data-mining-ta-materials/datasets/review.json'
    review_rdd = sc.textFile(review_path)

    stopwords_file = ""
    stopwords = []
    f = open(stopwords_file, 'r')
    for line in f:
        stopwords.append(line.strip())

    # print(review_rdd.getNumPartitions())
    # print(review_rdd.take(1))

    # review_rdd = review_rdd.map(lambda x: x.split(' ')).take(1)
    # print(review_rdd)

    # JSON format {review: XXX, user: CCC, year: 2017, ...}
    # review_rdd = review_rdd.map(lambda x: json.loads(x)).persist()  # RDD: List(dict())
    #
    # num_reviews = review_rdd.count()
    # print(num_reviews)
    #
    # num_reviews_2018 = review_rdd.map(lambda x: x['date'].split('-')[0])\
    #     .filter(lambda x: x == '2018').count()
    # print(num_reviews_2018)
    #
    # Task 2
    # business_rdd = (business_id, (bus_name, bus_loc))
    # review_rdd = (business_id, (user_id, date))
    # business_rdd.join(review_rdd) => (business_id, (bus_nmae, bus_loc, user_id, date)).map(lambda x: x[1][3])
    #
    # Task 3
    #
    # def func(x):
    #
    # spark: partitionBy(func)


if __name__ == '__main__':

    sc_conf = pyspark.SparkConf() \
        .setAppName('task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    main()
