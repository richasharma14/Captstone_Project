import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (StringType, IntegerType, TimestampType,DateType,FloatType)


   
# defining the function to load the file and then calling it last
def loadFile(filepath1,appName1):
    spark = SparkSession.builder.appName(appName1).getOrCreate()
    try:
    # Open the JSON file and read its contents
        with open(filepath1, 'r') as file:
            sp_df = spark.read.json(filepath1)
    except FileNotFoundError:
        print(f"File not found: {filepath1}")
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {filepath1}")
    except Exception as e:
        print(f"An error occurred while reading the JSON file: {e}")

    return sp_df

# defining a function to save in db and then calling it at last
def write_to_database(df_input, dtable,mode):
        df_input.write.format("jdbc").mode(mode) \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", dtable) \
            .option("user", "root") \
            .option("password", "password") \
            .save()


#Captsone_Project\Credit_Card\JSonFile\cdw_sapp_credit.json
# creating path variables for all three files
credit_json_file_path="Captsone_Project/Credit_Card/JSonFile/cdw_sapp_credit.json"
customer_json_file_path="Captsone_Project/Credit_Card/JSonFile/cdw_sapp_custmer.json"
branch_json_file_path="Captsone_Project/Credit_Card/JSonFile/cdw_sapp_branch.json"


# tranfoming according to the mapping doc 
#creating a function by giving df_customer argument
def transform_data(df_customer):
        df_customer = df_customer.withColumn("SSN", col("SSN").cast(IntegerType())) \
            .withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType())) \
            .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType())) \
            .withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType()))
        df_customer = df_customer.withColumn("FIRST_NAME", initcap(col('FIRST_NAME'))).withColumn("LAST_NAME", initcap(
            col('LAST_NAME')))
        df_customer = df_customer.withColumn('MIDDLE_NAME', lower(col('MIDDLE_NAME')))
        df_customer = df_customer.withColumn("FULL_STREET_ADDRESS",
                                             concat_ws(",", col('APT_NO'), col('STREET_NAME'))).drop("APT_NO").drop(
            "STREET_NAME")
        df_customer = df_customer.withColumn("CUST_PHONE",
                                             regexp_replace(df_customer.CUST_PHONE, "(\d{3})(\d{3})(\d{1})",
                                                            "($1) $2-$3"))
        return df_customer




        

# doing the same thing as above for branch_data n mappimg according to requirement
def transform_branch_data(df):
        df = df.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())) \
            .withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast(IntegerType())) \
            .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))
        df = df.withColumn("BRANCH_PHONE", regexp_replace(df.BRANCH_PHONE, "(\d{3})(\d{3})(\d{4})", "($1) $2-$3"))
        df = df.na.fill(00000, subset=["BRANCH_ZIP"])
        return df


# same thing creating function for transfom_credit_data n mapping according to requirement

def transform_credit_data(df_credit):
        
 df_credit = df_credit.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

    # Convert column types create new columns or transform existing columns within a DataFrame.
 df_credit = df_credit.withColumn("TRANSACTION_TYPE", col("TRANSACTION_TYPE").cast(StringType()))
 df_credit = df_credit.withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast(FloatType()))
 df_credit = df_credit.withColumn("CUST_CC_NO", col("CUST_CC_NO").cast(StringType()))
 #  It pads the value of the "MONTH" column with leading zeros,
 #  ensuring that it always has a width of two characters. For example,
 #  if the value is 5, it will be padded as "05".
 #The expression concatenates the values of three existing columns: "YEAR", "MONTH", and "DAY".
 df_credit = df_credit.withColumn("TIMEID", expr("concat(YEAR, LPAD(MONTH, 2, '0'), LPAD(DAY, 2, '0'))"))
 #df_credit = df_credit.withColumn("TIMEID", col("TIMEID").cast(DateType()))
 df_credit = df_credit.withColumn("TIMEID", expr("TO_DATE(TIMEID, 'yyyyMMdd')").cast(DateType()))

 df_credit = df_credit.drop("DAY", "MONTH", "YEAR")

 return df_credit

# 
#Req-1.1
#Data Extraction and Transformation with Python and 
#PySpark


# loading/reading the all three files
credit_df=loadFile(credit_json_file_path,'Credit')
customer_df=loadFile(customer_json_file_path ,'Custoner')
branch_df=loadFile(branch_json_file_path,'Branch')

#transforming the files
credit_df_transform=transform_credit_data(credit_df)
branch_df_transform=transform_branch_data(branch_df) 
df_custmer_transform=transform_data(customer_df)

df_custmer_transform.show()
column_names = df_custmer_transform.columns

# Print the column names
for column_name in column_names:
    print(column_name)   

#   
#Req-1.2
#Data loading into Database
#Create a Database in SQL(MySQL), named “creditcard_capstone.”
# with the table name as per requirement
#CDW_SAPP_BRANCH
#CDW_SAPP_CREDIT_CARD
#CDW_SAPP_CUSTOMER 
# by using overwrite method
#save to database 

df_credit_db=write_to_database(credit_df_transform,"creditcard_capstone.CDW_SAPP_CREDIT_CARD","overwrite")
df_branch_db=write_to_database(branch_df_transform,"creditcard_capstone.CDW_SAPP_BRANCH","overwrite")
df_custmer_db=write_to_database(df_custmer_transform,"creditcard_capstone.CDW_SAPP_CUSTOMER","overwrite")
