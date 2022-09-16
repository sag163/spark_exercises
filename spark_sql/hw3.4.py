from pyspark.sql import SparkSession
from pyspark.sql import functions as f



appName = "PySpark Task 1"
master = 'local[*]'
spark = SparkSession.builder.enableHiveSupport().appName(appName).getOrCreate()


# '''4. Напишите запрос, возвращающий фамилию, ID сотрудника, фамилию менеджера и ID менеджера для каждого сотрудника (для сотрудников, у которых нет менеджера, в этих колонках должен быть NULL). 
# Назовите колонки 'Employee', 'Emp#', 'Manager', 'Mgr#'. Результат сохранить в формате avro со сжатием Snappy'''


df_employees = spark.read.option("sep","\t").option("encoding","UTF-8").option('header', True).csv('/home/sag163/airflow/dags/data/employees/employees')
df_employees2 = spark.read.option("sep","\t").option("encoding","UTF-8").option('header', True).csv('/home/sag163/airflow/dags/data/employees/employees')

df_main = df_employees.join(df_employees2, df_employees2.EMPLOYEE_ID == df_employees.MANAGER_ID, 'left')

result = df_main.select(df_employees.EMPLOYEE_ID.alias("Employee"), df_employees.LAST_NAME.alias("Emp#"), df_employees2.LAST_NAME.alias("Manager"), df_employees.MANAGER_ID.alias("Mgr#"))
result.show()



result.write.format("avro").mode("overwrite"). save("/home/sag163/airflow/dags/result3.4/")


# Запуск: spark-submit --jars spark-avro_2.12-3.3.0.jar hw3.4.py 
