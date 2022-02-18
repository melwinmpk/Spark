from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('PySpark MySql Intgn')\
            .master('local[*]')\
            .config('spark.jars','file:///home/saif/jars/mysql-connector-java-8.0.27.jar')\
            .getOrCreate()

    empDf = spark.read.format("jdbc")\
            .option("url","jdbc:mysql://localhost:3306/retail_db?useSSL=false")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("dbtable","emp")\
            .option("user","root")\
            .option("password","Welcome@123")\
            .load()
    empDf.show()

    empDf.createOrReplaceTempView('emp')
    resultdf = spark.sql("""
    SELECT 	A.EMPNO AS EMP_EMPNO, A.ENAME AS EMP_NAME, B.EMPNO AS MGR_EMPNO, B.ENAME AS MGR_NAME, C.EMP_CNT AS MGR_TEAM_CNT FROM emp A, emp B, (SELECT A.MGR, COUNT(A.ENAME) EMP_CNT FROM emp A, emp B WHERE A.MGR=B.EMPNO GROUP BY A.MGR) C WHERE A.MGR=B.EMPNO AND B.EMPNO=C.MGR ORDER BY B.EMPNO;
    """)

    resultdf.write.format("jdbc")\
            .mode("overwrite")\
            .option("url","jdbc:mysql://localhost:3306/retail_db?useSSL=false")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("dbtable","emp_mgr")\
            .option("user","root")\
            .option("password","Welcome@123")\
            .save()

    resultdf.show()