from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('read-write Hive Managed Table') \
        .master('local[3]')\
        .config('hive.metastore.uris', 'thrift://localhost:9083/')\
        .config('spark.sql.warehouse.dir', 'hdfs://localhost:9000/user/saif/warehouse')\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql('use cohert_c8')
    spark.sql('show tables').show()



    # old way of doing
    # sc = SparkContext(appName="test")
    # sqlContext = HiveContext(sc)
    # sqlContext.sql("use cohert_c8")
    # sqlContext.sql('show tables').show()