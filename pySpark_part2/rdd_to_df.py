from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sqlCon = SQLContext(sc)
    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)]
    rdd = sc.parallelize(dept)

    #df = rdd.toDF()
    df = rdd.toDF(['deptName', 'DeptID']) # setting the Column  name
    df.show()
