from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName('Joins') \
            .master('local[*]')\
            .getOrCreate()

    emp = [(1, "Saif", -1, "2018", "10", "M", 3000),
           (2, "Ram", 1, "2010", "20", "M", 4000),
           (3, "Aniket", 1, "2010", "10", "M", 1000),
           (4, "Mitali", 2, "2005", "10", "F", 2000),
           (5, "Nahid", 2, "2010", "40", "", -1),
           (6, "Sufiyan", 2, "2010", "50", "", -1)]
    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id",
                  "gender", "salary"]
    empDF = spark.createDataFrame(data=emp, schema=empColumns)
    empDF.printSchema()
    empDF.show(truncate=False)
    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)]
    deptColumns = ["dept_name", "dept_id"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    deptDF.printSchema()
    deptDF.show(truncate=False)

    InnerJoinDf = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'inner')
    InnerJoinDf.show()

    InnerJoinDf2 = empDF.alias('e').join(
                        deptDF.alias('d'),
                        col('e.emp_dept_id') == col('d.dept_id'),
                        'inner'
    )
    InnerJoinDf2.show()

    InnerJoinDf3 = empDF.alias('e').join(
        deptDF.alias('d'),
        col('e.emp_dept_id') == col('d.dept_id'),
        'leftsemi'
    )
    InnerJoinDf3.show()

    InnerJoinDf4 = empDF.alias('e').join(
        deptDF.alias('d'),
        col('e.emp_dept_id') == col('d.dept_id'),
        'leftanti'
    )
    InnerJoinDf4.show()

