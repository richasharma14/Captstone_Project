from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.master("local[1]").appName('Credit_Card').getOrCreate()
if spark is not None and spark._sc is not None:
    print("Database connection successful")
else:
    print("Failed to establish a database connection")
    

mysql_properties = {
    "driver": "com.mysql.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    }


def f():

    while True:
        print("*" * 50)
        print("Application Front-End Menu")
        print("*" * 50)
        N =input("Enter 1 for Transaction Details Module , 2 for Customer Details Module \n")
        try: 
            N = int(N)
            if(N==1):
             try:
                X =input("Enter 1 for transactions made by customers ,2 number and total values of transactions for a given type ,3 for total number and total values of transactions for branches , 4 for quit \n")
                X = int(X)
                if (X==1):
                   customer_transaction()
                elif(X==2):
                   transaction_details()
                elif(X==3):
                    #Used to display the total number and total values of transactions for branches in a given state
                   branch_transaction_details()
                break
             except:
                print("you did not enter any number")
            elif(N==2):
             try:
                t =input("Enter 1 account details of a customer  , 2 for  modify the existing account details of a customer..., 3 for generate a monthly bill for a credit card number for a given month and year, 4 for  transactions made by a customer between two dates\n")
                T=int(t)
                if(T==1):
                 customer_details()
                elif(T==2):
                 update_customer_details()
                elif(T==3):
                   credit_card_bill()
                elif(T==4):
                   transactions_detials_date()
             except:
                 print("you did not enter any number")
                

            
            else:
                break
        except:
            print("you did not enter any number")



def update_customer_details():
   ssn= input('Enter the Customer SSN without hyphens\n')
   try: 
     update_customer_data_detials(ssn)   
   except:
       print('Please enter the correct info')

def update_customer_data_detials(ssn):
    print('Inside the update_customer_data_detials method')
    print(f'SSN={ssn}')

    query = f"SELECT * \
        FROM creditcard_capstone.cdw_sapp_customer \
        WHERE SSN = {ssn}"
    
    print("Query:", query)
    
    try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
    except Exception as e:
        print("Error executing query:", str(e))
    df.show()
    try:
       column_name = input("Enter the name of the column you want to update: ")
       new_value = input("Enter the new value: ")
       filtered_df = df.filter(df.SSN == ssn)
       updated_df = filtered_df.withColumn(column_name, lit(new_value))
       updated_df.show()
    #    updated_df.write.format("jdbc").options(**mysql_properties).option("dbtable", "CDW_SAPP_CUSTOMER").mode("overwrite").save()
    #    df.exceptAll(updated_df).write.format("jdbc").options(**mysql_properties).option("dbtable", "CDW_SAPP_CUSTOMER").mode("append").save()
       
       
       
       #df.write.format("jdbc").options(**mysql_properties).option("dbtable", "CDW_SAPP_CUSTOMER").mode("overwrite").save()
       #updated_df.write.format("jdbc").options(**mysql_properties).option("dbtable", "CDW_SAPP_CUSTOMER").mode("overwrite").save()
    except:
       print('Somthing went wrong.Please call customer care at 800-123-HAHA')
    # total_transaction_value = df.first()["total_transaction_value"]
    # print("Total Transaction Value:", total_transaction_value)
    # Show the resulting DataFrame
    
    #print('After executing the query')


def transactions_detials_date():
    ssn= input('Enter the Customer SSN \n')
    startdate= input('Enter start date of trancation \n')
    endate=input('Enter end date  of trancation \n')
    try:
     
     get_transactions_detials_date(ssn,startdate,endate)
       
    except:
       print('Please enter the correct info') 



def get_transactions_detials_date(ssn,startdate,endate):
    print('Inside the get_transactions_detials_date method')
    print(f'SSN={ssn}, StartDate={startdate}, EndDate={endate}')

    query = f"SELECT * \
        FROM creditcard_capstone.cdw_sapp_credit_card \
        WHERE CUST_SSN = {ssn} \
        AND DATE(TIMEID) BETWEEN '{startdate}' AND '{endate}' \
        ORDER BY YEAR(TIMEID) DESC, MONTH(TIMEID) DESC, DAY(TIMEID) DESC "
    
    print("Query:", query)
    query1 = "SELECT 1"
    try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
    except Exception as e:
        print("Error executing query:", str(e))
    df.show()
    # total_transaction_value = df.first()["total_transaction_value"]
    # print("Total Transaction Value:", total_transaction_value)
    # Show the resulting DataFrame
    
    #print('After executing the query')


def customer_details():
   print('Inside the customer_details method')
   

   query = "SELECT * FROM creditcard_capstone.cdw_sapp_customer"

   try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
   except Exception as e:
        print("Error executing query:", str(e))
    
    # Show the resulting DataFrame
   df.show()
   

def credit_card_bill():
    x1= input('Enter the credit card number without spaces \n')
    x2= input('Enter month of trancation \n')
    x3=input('Enter year of trancation \n')
    try:
        
        x2 = int(x2)
        x3 = int(x3)
        get_credit_card_bill_transactions(x1,x2,x3)
       
    except:
       print('Please enter the correct zip code') 
def get_credit_card_bill_transactions(creditcard, month, year):
    print('Inside the get_credit_card_bill_transactions method')
    print(f'zipcode={creditcard}, month={month}, year={year}')

    query = f"SELECT SUM(TRANSACTION_VALUE) AS total_transaction_value \
          FROM creditcard_capstone.cdw_sapp_credit_card \
          WHERE CUST_SSN = '{creditcard}' \
         AND MONTH(TIMEID) = {month} \
         AND YEAR(TIMEID) = {year}"
    
    print("Query:", query)
    query1 = "SELECT 1"
    try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
    except Exception as e:
        print("Error executing query:", str(e))
    total_transaction_value = df.first()["total_transaction_value"]
    print("Total Transaction Value:", total_transaction_value)
    # Show the resulting DataFrame
    df.show()
    #print('After executing the query')


def customer_transaction():
    x1= input('Enter the zip code \n')
    x2= input('Enter month of trancation \n')
    x3=input('Enter year of trancation \n')
    try:
        x1 = int(x1)
        x2 = int(x2)
        x3 = int(x3)
        if x1<10000 or x1>999999:
          print('Please enter the valid zip code')
        else:
           get_customer_transactions(x1,x2,x3)
       
    except:
       print('Please enter the correct zip code') 


def transaction_details():
    y1= input('Enter the transactions type \n')
    try:
        
        get_transactions_detials(y1)
       
    except:
       print('Please enter the transactions')  

def branch_transaction_details():
    state= input('Enter the state for whcih you want to know the detials \n')
    try:
        get_branchtransactions_detials(state)
       
    except:
       print('Please enter the correct state')

# spark = SparkSession.builder.master("local[1]").appName('Credit_Card').getOrCreate()
# if spark is not None and spark._sc is not None:
#     print("Database connection successful")
# else:
#     print("Failed to establish a database connection")
    

# mysql_properties = {
#     "driver": "com.mysql.jdbc.Driver",
#     "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
#     "user": "root",
#     "password": "password",
#     }

def get_customer_transactions(zipcode, month, year):
    print('Inside the get_customer_transaction method')
    print(f'zipcode={zipcode}, month={month}, year={year}')

    query = f"SELECT c.CUST_ZIP, c.FIRST_NAME, c.LAST_NAME, cc.TRANSACTION_VALUE, cc.TRANSACTION_TYPE, DAYOFMONTH(cc.TIMEID) AS day_value \
              FROM creditcard_capstone.cdw_sapp_customer c \
              JOIN creditcard_capstone.cdw_sapp_credit_card cc ON c.SSN = cc.CUST_SSN \
              WHERE c.CUST_ZIP = {zipcode} AND MONTH(cc.TIMEID) = {month} AND YEAR(cc.TIMEID) = {year} \
              ORDER BY DAYOFMONTH(cc.TIMEID)"
    
    print("Query:", query)
    query1 = "SELECT 1"
    try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
    except Exception as e:
        print("Error executing query:", str(e))
    
    # Show the resulting DataFrame
    df.show()
    #print('After executing the query')

def get_transactions_detials(transactiontype):
    print('Inside the get_transactions_detials method')
    print(f'transactionstype={transactiontype}')

    query = f"SELECT c.CUST_ZIP,c.SSN,cc.CUST_SSN,cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE,cc.TIMEID \
             FROM creditcard_capstone.cdw_sapp_customer c  JOIN creditcard_capstone.cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN \
            where cc.TRANSACTION_TYPE = '{transactiontype}'"
    
    print("Query:", query)
    query1 = "SELECT 1"
    try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
    except Exception as e:
        print("Error executing query:", str(e))
    
    count = df.count()

# Total value
    total_value = df.selectExpr("SUM(TRANSACTION_VALUE)").first()[0]

    print("Count:", count)
    print("Total Value:", total_value)
    # Show the resulting DataFrame
    df.show()
    #print('After executing the query')

def  get_branchtransactions_detials(state):
    print('Inside the get_branchtransactions_detials method')
    print(f'transactionstype={state}')
    query = f"SELECT bc.BRANCH_CODE, bc.BRANCH_STATE, SUM(cc.TRANSACTION_VALUE) AS total_value, COUNT(*) AS total_transactions \
           FROM creditcard_capstone.cdw_sapp_branch bc  \
          JOIN creditcard_capstone.cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE \
          WHERE bc.BRANCH_STATE = '{state}'  \
          GROUP BY bc.BRANCH_CODE, bc.BRANCH_STATE"
    
    print("Query:", query)

    try:
        df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
    except Exception as e:
        print("Error executing query:", str(e))
# Show the resulting DataFrame
    df.show()

f()

