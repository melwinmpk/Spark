from pyspark import SparkConf
from pyspark import SparkContext

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    ordersfile = sc.textFile("file:///home/saif/LFS/datasets/orders.txt")
    print(ordersfile.collect())

