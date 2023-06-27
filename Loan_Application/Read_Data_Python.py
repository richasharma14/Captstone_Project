import requests
import pyspark
import json
from pprint import pp
import pyspark
from pyspark.sql import SparkSession

# 4.1 Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
# here i usung base url 
baseurl='https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
#Find the status code of the above API endpoint.

#Hint: status code could be 200, 400, 404, 401
 # getting a response r back by using requets .get method
r = requests.get(baseurl)
# here  i creaet a status code variabe from response i m checking its status code and it will show me 200/400/500 or any error
status_code = r.status_code

#  4.2 Print the status code 
print(f'Status code: {status_code}')

print(r.status_code)
#  here getting data from response in json 
data = r.json()
#print(data)
pp(data)
# 4.3 Once Python reads data from the API, utilize PySpark to load data into RDBMS (SQL). The table name should be CDW-SAPP_loan_application in the database.

#: Use the “creditcard_capstone” database.

# here i creating a spark session 
spark = SparkSession.builder.master("local[1]").appName('ApiData.com').getOrCreate()
# creating a rdd
#  creates an RDD by distributing the provided data across the cluster, 
# making it available for parallel processing using Spark's distributed computing capabilities.
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF()
df.show()
# saving the df to the db n lastly i m printing data loaded to the db
df.write.format("jdbc").mode("overwrite")\
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
        .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application")\
        .option("user", "root")\
        .option("password", "password")\
        .save()
print('Data loaded from file and saved in DB')

