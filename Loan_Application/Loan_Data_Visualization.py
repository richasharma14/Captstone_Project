from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt

# here first i m creating a spark session

spark = SparkSession.builder.master("local[1]").appName('ApiData.com').getOrCreate()
 # connecting it to my sql
mysql_properties = {
    "driver": "com.mysql.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
}

# Read data from a MySQL table
# reading my data from loan application using spark session that i created earlier
df = spark.read \
    .format("jdbc") \
    .options(**mysql_properties) \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
    .load()

# Perform operations on the DataFrame (e.g., select, filter, etc.)
df.show(5)
# creating a new variable name and reading data from credit card using spark session
credit_df = spark.read \
    .format("jdbc") \
    .options(**mysql_properties) \
    .option("dbtable", "creditcard_capstone.cdw_sapp_credit_card") \
    .load()

# Perform operations on the DataFrame (e.g., select, filter, etc.)
credit_df.show(5)


# 5.1 Find and plot the percentage of applications approved for self-employed applicants.
#Note: Take a screenshot of the graph. 
#data_api = df.groupby(['Self_Employed','Application_Status'])['Application_ID'].count()
#data_api = df.groupby(['Self_Employed', 'Application_Status']).agg({'Application_ID': 'count'})
from pyspark.sql import functions as F

# creating a new df named selected_df .
# on the df i m applying select method
selected_df = df.select("Self_Employed","Application_Status","Application_ID")
#on selected_df i m using filter method to just get only self_employed == yes
filtered_df = selected_df.filter(selected_df['Self_Employed'] == 'Yes')
#now on filterd_df i m using again filter method to just select application _status col==y n then using count method to get the 
# count of total approved_count

approved_count = filtered_df.filter(filtered_df['Application_Status'] == 'Y').count()
total_count = filtered_df.count()
# here i m getting % of approval
approval_percentage = (approved_count / total_count) * 100

# # Create a DataFrame to hold the result
# result_df = pd.DataFrame({
#     'Application_Status': ['Approved', 'Rejected'],
#     'Percentage': [approval_percentage, 100 - approval_percentage]
# })

# # Plotting the bar graph
# plt.bar(result_df['Application_Status'], result_df['Percentage'], color=['green', 'red'])
# plt.title('Percentage of Applications Approved for Self-Employed Applicants')
# plt.ylabel('Percentage')
# plt.ylim(0, 100)

# plt.show()
# creating bar garph by providing values that i draw from up
x_values = ['Approved', 'Rejected']
y_values = [approval_percentage, 100 - approval_percentage]

plt.bar(x_values, y_values, color=['red', 'green'])
plt.title('Percentage of Approval for self-employed')
plt.ylabel('Percentage')
plt.ylim(0, 100)

plt.show()
# data_api = df.groupby(['Self_Employed', 'Application_Status']).agg(F.count('Application_ID').alias('Count'), F.collect_list('Application_ID').alias('Application_IDs'))
# #data_api = df.reset_index()
# #total_applications = len(df)
# row_count = df.count()
# data_api = data_api[(df['Application_Status'] == 'Y') & (df['Self_Employed'] == 'Yes' ) ]
# print(data_api)
# print(row_count)
# print(data_api.Count)
# count_value = data_api.select('Count').collect()[0][0]
# print(f"Count value: {count_value}")

# #data_api_perc = data_api['count(Application_ID)'] / row_count * 100
# data_api_perc = count_value / row_count * 100
# print("Percentage of applications approved for self-employed applicants: ", data_api_perc)
# x_values = data_api['Application_Status']
# y_values = data_api['Count']
# hue_values = data_api['Self_Employed']

# sns.set(rc={"figure.figsize": (4, 6)})
# sns.set_theme(style="whitegrid", palette="hls")
# sns.barplot(x=x_values, y=y_values, hue=hue_values, data=data_api)
# # sns.barplot(x=data_api['Application_Status'],
# #             y='data_api_perc',
# #             hue=data_api['Self_Employed'],
# #             data=data_api).set(title="Percentage of applications approved for self-employed applicants.")
# plt.show()

# data_api = df.groupby(['Self_Employed', 'Application_Status']).agg(F.count('Application_ID').alias('Count'), F.collect_list('Application_ID').alias('Application_IDs'))
# row_count = df.count()

# data_api = data_api[(data_api['Application_Status'] == 'Y') & (data_api['Self_Employed'] == 'Yes')]

# count_value = data_api.select('Count').collect()[0][0]
# data_api_perc = count_value / row_count * 100
# print(data_api_perc)
# # x_values = data_api['Application_Status']
# # print(x_values)
# # y_values = data_api['Count']
# # hue_values = data_api['Self_Employed']
# x_values = data_api.select('Application_Status').rdd.flatMap(lambda x: x).collect()
# print(x_values)
# y_values = data_api.select('Count').rdd.flatMap(lambda x: x).collect()
# print(y_values)
# hue_values = data_api.select('Self_Employed').rdd.flatMap(lambda x: x).collect()
# print(hue_values.print)

# sns.set(rc={"figure.figsize": (11, 8)})
# sns.set_theme(style="whitegrid", palette="hls")
# sns.barplot(x=x_values, y=y_values, hue=hue_values, data=data_api)
# plt.title("Percentage of applications approved for self-employed applicants.")
# plt.show()

# print("Percentage of applications approved for self-employed applicants: ", data_api_perc)


##########################################################################################################
#Note: Take a screenshot of the graph.
# 5.2 Find the percentage of rejection for married male applicants.
# creating a new df by using select method on df to get married,app_status,grnder,app_id
selected_df = df.select("Married","Application_Status","Gender","Application_ID")
selected_df.show()
selected_df.count()
#crating a new variable fillter_df by using filter method on selected_df to get 
filtered_df = selected_df.filter((selected_df['Married'] == 'Yes') & (selected_df['Gender'] == 'Male'))

rejected_count = filtered_df.filter(filtered_df['Application_Status'] == 'N').count()
total_count = filtered_df.count()

rejection_percentage = (rejected_count / total_count) * 100

print("Percentage of rejection for married male applicants:", rejection_percentage)

x_values = ['Rejection', 'Approval']
y_values = [rejection_percentage, 100 - rejection_percentage]

plt.bar(x_values, y_values, color=['red', 'green'])
plt.title('Percentage of Rejection for Married Male Applicants')
plt.ylabel('Percentage')
plt.ylim(0, 100)

plt.show()

##########################################################################################################
#Find and plot the top three months with the largest transaction data.
query = "SELECT YEAR(TIMEID) AS year, MONTH(TIMEID) AS month, SUM(TRANSACTION_VALUE) AS total_transaction " \
        "FROM creditcard_capstone.cdw_sapp_credit_card " \
        "GROUP BY YEAR(TIMEID), MONTH(TIMEID) " \
        "ORDER BY total_transaction DESC " \
        "LIMIT 3"

# Read the data from MySQL using Spark
credit_df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()

# Show the resulting DataFrame
credit_df.show()

df_pandas = credit_df.toPandas()

# Extract year, month, and total_transaction columns
year = df_pandas["year"]
month = df_pandas["month"]
total_transaction = df_pandas["total_transaction"]

# # Create a bar plot
# plt.figure(figsize=(10, 6))
# plt.plot(month, total_transaction, ls='-', c='b', lw='2', marker='x')
# #plt.bar(month, total_transaction)
# plt.xlabel("Month")
# plt.ylabel("Total Transaction")
# plt.title("Top Three Months with Largest Transaction Data")
# plt.xlim(min(month), max(month))
# #plt.xticks(rotation=0)
# plt.tight_layout()

# # Display the plot
# plt.show()

plt.rcParams["figure.figsize"] = [8, 5]
plt.rcParams["figure.autolayout"] = True

# Define month numbers and corresponding transaction values
x = [5, 10, 12]  # May, October, December (example month numbers)
#transaction_values = df_months['TRANSACTION_VALUE'][:3]  # Transaction values for May, October, December (example)

# Use month names as labels
month_names = ['May', 'Oct', 'Dec']  # Replace with actual month names

# Create the plot
plt.plot(x, total_transaction, ls='-', c='g', lw='3', marker='o')
plt.title('Top Three Months With Highest Number Of Transactions')
plt.xticks(x, month_names)  # Set x-axis labels to month names
plt.ylabel('Transaction Amount')
plt.xlabel('Months')

# Display the plot
plt.show()

##############################################################################################
#Find and plot which branch processed the highest total dollar value of healthcare transactions.

query = "SELECT bc.BRANCH_CODE, SUM(cc.TRANSACTION_VALUE) AS TOTAL_VALUE \
         FROM creditcard_capstone.cdw_sapp_branch bc \
         JOIN creditcard_capstone.cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE \
         WHERE cc.TRANSACTION_TYPE = 'Healthcare' \
         GROUP BY bc.BRANCH_CODE \
         ORDER BY TOTAL_VALUE DESC \
         LIMIT 5"
healtcare_df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
#df = spark.sql(query).toPandas()
df_pandas = healtcare_df.toPandas()
# Plot the results
plt.bar(df_pandas.index, df_pandas['TOTAL_VALUE'])
#plt.bar(df_pandas['BRANCH_CODE'], df_pandas['TOTAL_VALUE'])
plt.title('Branch with Highest Total Dollar Value of Healthcare Transactions')
plt.xlabel('Branch Code')
plt.xticks(df_pandas.index, df_pandas['BRANCH_CODE'])
plt.ylabel('Total Dollar Value')
plt.show()
# spark_df = spark.createDataFrame(df)
# spark_df = spark_df.select('BRANCH_CODE', 'TRANSACTION_VALUE').filter(spark_df.TRANSACTION_TYPE == 'Healthcare')
# panda_df = spark_df.toPandas()
# df = panda_df.groupby('BRANCH_CODE')['TRANSACTION_VALUE'].sum().reset_index()
# df = df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
# df = df[:5]
# df_top5 = df.sort_values(by=['BRANCH_CODE'], ascending=True)
# print(df)

# top_five = df_top5['TRANSACTION_VALUE']
# colors = ['grey' if (s < max(top_five)) else 'red' for s in top_five]

# fig, ax = plt.subplots(figsize=(6, 5))
# sns.set_style('white')
# ax = sns.barplot(x='BRANCH_CODE', y='TRANSACTION_VALUE',
# data=df_top5, palette=colors)
# plt.title('Branch which has Highest Number Of Healthcare Transactions', fontsize=12)
# plt.xlabel('Branch Code')
# plt.xticks(fontsize=16)
# plt.ylabel('Transaction Amount', fontsize=12)
# plt.yticks(fontsize=15)
# ax.text(x=0.9, y=0.9, s='Branch 25 with highest total healthcare transaction',color='red', size=10, weight='bold')
# sns.despine(bottom=True)
# ax.grid(False)
# ax.tick_params(bottom=False, left=True)
# plt.show()

##################################################################################################
#Find and plot which transaction type has a high rate of transactions.

query = "SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT \
         FROM creditcard_capstone.cdw_sapp_credit_card \
         GROUP BY TRANSACTION_TYPE \
         ORDER BY TRANSACTION_COUNT DESC"

#df = spark.sql(query).toPandas()
highest_df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
#df = spark.sql(query).toPandas()
df1_pandas = highest_df.toPandas()
# Calculate transaction rates
total_transactions = df1_pandas['TRANSACTION_COUNT'].sum()
df1_pandas['TRANSACTION_RATE'] = df1_pandas['TRANSACTION_COUNT'] / total_transactions

# Plot the transaction rates
plt.bar(df1_pandas['TRANSACTION_TYPE'], df1_pandas['TRANSACTION_RATE'])
plt.title('Transaction Types with High Rates of Transactions')
plt.xlabel('Transaction Type')
plt.ylabel('Transaction Rate')

# Rotate x-axis labels for better visibility
plt.xticks(rotation=45)

plt.show()

##############################################################################
#Find and plot which state has a high number of customers.
query = "SELECT CUST_STATE, COUNT(DISTINCT SSN) AS CUSTOMER_COUNT \
         FROM creditcard_capstone.cdw_sapp_customer \
         GROUP BY CUST_STATE \
         ORDER BY CUSTOMER_COUNT DESC"
highest_df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
#df = spark.sql(query).toPandas()
df2_pandas = highest_df.toPandas()
plt.bar(df2_pandas['CUST_STATE'], df2_pandas['CUSTOMER_COUNT'])
plt.title('States with High Number of Customers')
plt.xlabel('State')
plt.ylabel('Customer Count')

# Rotate x-axis labels for better visibility
plt.xticks(rotation=45)

plt.show()

########################################################################################################
# Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
# Hint (use CUST_SSN).
query = "SELECT c.FIRST_NAME, c.LAST_NAME, c.MIDDLE_NAME, c.SSN, c.CUST_EMAIL, cc.CUST_SSN, SUM(cc.TRANSACTION_VALUE) AS TOTAL_TRANSACTION \
         FROM creditcard_capstone.cdw_sapp_customer c \
         JOIN creditcard_capstone.cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN \
         GROUP BY c.FIRST_NAME, c.LAST_NAME, c.MIDDLE_NAME, c.SSN, c.CUST_EMAIL, cc.CUST_SSN \
         ORDER BY TOTAL_TRANSACTION DESC \
         LIMIT 10"

#df = spark.sql(query).toPandas()
highest1_df = spark.read.format("jdbc").options(**mysql_properties).option("query", query).load()
#df = spark.sql(query).toPandas()
df3_pandas = highest1_df.toPandas()
# Plot the sum of transactions for the top 10 customers
plt.bar(range(len(df3_pandas)), df3_pandas['TOTAL_TRANSACTION'])
plt.xticks(range(len(df3_pandas)), df3_pandas['SSN'])
#plt.bar(df3_pandas['CUST_SSN'], df3_pandas['TOTAL_TRANSACTION'])
plt.title('Sum of Transactions for Top 10 Customers')
plt.xlabel('Customer SSN')
plt.ylabel('Total Transaction Amount')

# Find the customer with the highest transaction amount
max_transaction_customer = df3_pandas.loc[df3_pandas['TOTAL_TRANSACTION'].idxmax()]
print("Customer with the highest transaction amount:")
print(max_transaction_customer)

plt.show()