
from pyspark import SparkConf, SparkContext

# 编程01：
# 已知：list01 = ["My name is John", "How are you"]
# （1）请编程计算list01中各个元素的长度。输出格式：[15, 11]
# （2）请编程计算list01中各个元素中各个单词的长度。输出格式：
# [(‘My’, 2), (‘name’, 4), (‘is’, 2), (‘John’, 4), (‘How’, 3), (‘are’, 3), (‘you’, 3)]
conf = SparkConf().setMaster("local[*]").setAppName("app")
sc=SparkContext(conf=conf)
#
print("--------------")
list01 = ["My name is John", "How are you"]
rdd01 = sc.parallelize(list01)
rdd_map = rdd01.map(lambda x:len(x))
print(rdd_map.collect())
rdd_flatmap = rdd01.flatMap(lambda x:x.split(' '))
rdd_02 = rdd_flatmap.map(lambda x:(x,len(x)))
print(rdd_02.collect())

# 编程02：
# 已知：numList = [[1, 3], [2, 4], [3, 5], [4, 6]]
# 请编程输出： [(1, 30), (2, 40), (3, 50), (4, 60)]

numList = [[1, 3], [2, 4], [3, 5], [4, 6]]
rdd = sc.parallelize(numList)
# rdd_
# 编程03：
# 已知：今天（today.csv）和昨天(yesterday.csv)的点名册。请编程找出：今天和昨天都旷课的学生。
# 输出格式 ：[‘李四, 旷课’,  ‘小明, 旷课’]




# 编程04：
# （1）创建一个文件夹，然后用VSCode打开该文件夹
# （2）创建一个虚拟环境，并激活该虚拟环境
# （3）新建一个py文件，然后写上一段Spark代码。写的过程中，如果缺少哪个模块或包，就使用pip install命令进行安装
# （4）在VSCode中运行该代码
# （5）如果要把你写的代码传给你同学或者上传到github，接下来该怎么做？

# rdd = sc.parallelize([1,2,3,4,5,6],4)
# print("默认分区数：",rdd.getNumPartitions())


# 编程05：
# sales.csv文件中是某小型超市的销售记录。请编程：找出销售量排名前3的商品及其销售数量。
# 输出格式：[('日记本', 25), ('笔芯', 20), ('啤酒', 12)]




# 编程06：
# 已知：list01= [10, 11, 12, 13, 14, 15],
# 请编程找出该列表中的偶数并仍以列表返回，同时通过累加器变量返回偶数的个数



# 编程07：
# 已知：
# rank = { "1":"优秀", "2":"中等", "3":"及格", "4":"不及格" }
# scores = [("张三","92","1"), ("李四","62","3"), ("王五","79","2"), ("赵柳","55","4")]。
# 请利用广播变量编程，将sores列表关联到rank字典，然后以下列格式输出：
# [('张三', '92', '优秀'), ('李四', '62', '及格'), ('王五', '79', '中等'), ('赵柳', '55', '不及格')]4745416




