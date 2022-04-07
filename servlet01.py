

from pyspark import SparkConf, SparkContext

# if __name__ == '__main__':
#     # conf = SparkConf().setMaster("local").setAppName("app")
#     # sc = SparkContext(conf=conf)
#     # print("1111")
#     # print(sc.parallelize([1, 2, 3]).collect())
#

conf = SparkConf().setMaster("local[*]").setAppName("app")
sc=SparkContext(conf=conf)
rdd = sc.parallelize([1,2,3,4,5,6],4)
print("默认分区数：",rdd.getNumPartitions())
