from pyspark.sql import SparkSession
from pyspark.sql import functions as f



appName = "PySpark Task 1"
master = 'local[*]'
spark = SparkSession.builder.enableHiveSupport().appName(appName).getOrCreate()


# '''3. Напишите запрос, возвращающий фамилию, ID отдела и наименование отдела для каждого сотрудника; Результат сохранить в формате avro со сжатием GZIP'''


df_employees = spark.read.option("sep", "\t").option("encoding", "UTF-8").option(
    'header', True).csv('/home/sag163/airflow/dags/data/employees/employees')
df_departments = spark.read.option("sep", ",").option("encoding", "UTF-8").option(
    'header', True).csv('/home/sag163/airflow/dags/data/departments/departments')

df_main = df_employees.join(
    df_departments, df_employees.DEPARTMENT_ID == df_departments.DEPARTMENT_ID)

result = df_main.select(df_employees.LAST_NAME,
                        df_employees.DEPARTMENT_ID, df_departments.DEPARTMENT_NAME)

result.show()

result.write.format("avro").mode("overwrite").option("codec","gzip"). save(
    "/home/sag163/airflow/dags/result/")


# Запуск: spark-submit --jars spark-avro_2.12-3.3.0.jar hw3.3.py 