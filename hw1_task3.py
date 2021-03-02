# import argparse
# import pyspark
#
#
# def gen_candidate_next_k(freqient_k):
#     union(frequent_k)
#
#     return candidate_next_k
#
#
# def find_frquent_k(candidate_k, dataset):
#     for data in dataset
#         count(candidate_k)
#
#     if num > args.support:
#         return candidate
#
# def main():
#
#     if:
#         rdd = [[bus1, bus2, bus3], [bus2, bus3], [bus5, bus3]]
#     else:
#
#     candidate_k = {} set([]) union
#
#     frequent_itemset = []
#     k = 1
#     while len(candidate) != 0:
#
#         frequent_k = candidate_k.map().collect()
#         frequent_itemset.append(frequent_k)
#         k = k + 1
#         candidate_k => rdd
#
#
#     return frequent_itemset
#
#
# if __name__ == '__main__':
#
#     parser = argparse.ArgumentParser(description='Apriori')
#     parser.add_argument('--input_file', type=str, default='../datasets/user_business.csv', help='input data path')
#     parser.add_argument('--output_file', type=str, default='../datasets/output', help='output data path')
#     parser.add_argument('--thre', type=int, default=100, help='threshold for qualified users')
#     parser.add_argument('--support', type=int, default=45, help='the minimum count for a frequent itemset')
#     parser.add_argument('--case', type=int, default=1, help='the case number | default = 1 for Task 2')
#
#     args = parser.parse_args()
#
#     sc_conf = pyspark.SparkConf() \
#         .setAppName('task1') \
#         .setMaster('local[*]') \
#         .set('spark.driver.memory', '8g') \
#         .set('spark.executor.memory', '4g')
#
#     sc = pyspark.SparkContext(conf=sc_conf)
#     sc.setLogLevel("OFF")
#
#     main()
