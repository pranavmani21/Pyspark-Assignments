
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_timestamp
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import date_format
#creating a dataframe
def createDataFrame():
    spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()
    schema = StructType([
        StructField("Product Name", StringType(), True),
        StructField("Issue Date", StringType(), True),
        StructField("Price", IntegerType(), True),
        StructField("Brand", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Product Number",StringType())
    ])

    data = [("Washing Machine", 1648770933000 , 20000, "Samsung", "India", "0001"),
            ("Refrigerator ", 1648770999000 , 35000, "  LG", None, "0002"),
            ("Air Cooler ", 1648770948000 , 45000, "    Voltas", None, "0003")
           ]

    df= spark.createDataFrame(data, schema)
    display(df)
    return df

createDataFrame()


# COMMAND ----------

#method to convert unix to timestamp
from pyspark.sql.functions import from_unixtime

def convert_unix_timestamp(df, column_name):
    df2 = df.withColumn(column_name, from_unixtime(df[column_name] / 1000, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ'))
    display(df2)

convert_unix_timestamp(df,"Issue Date")


# COMMAND ----------

#date conversion
from pyspark.sql.functions import from_unixtime,date_format

def add_date_column(df, new_col_name,col_name):
    df3 = df.withColumn(new_col_name, date_format(col_name, "yyyy-MM-dd"))
    display(df3)
    return df3

add_date_column(df2,"date","Issue Date")


# COMMAND ----------

#remove extra space
from pyspark.sql.functions import trim

def trim_name(df, new_column):
    df4 = df.withColumn(new_column, trim(df[new_column]))
    df4.show()
    return df4

trim_name(df3,"Brand")

# COMMAND ----------

#to remove null
def remove_null(df):
    df5=df.fillna('')
    display(df5)
    return df5

remove_null(df4)

# COMMAND ----------

#New Question
#create a dataframe 
def createDataFrame2():
    schema = StructType([
        StructField("SourceId", IntegerType(), True),
        StructField("TransactionNumber", IntegerType(), True),
        StructField("Language", StringType(), True),
        StructField("ModelNumber", IntegerType(), True),
        StructField("StartTime", StringType(), True),
        StructField("Product Number",StringType())
    ])

    data = [(150711,123456,"EN", 123456, "2021-12-27T08:20:29.842+0000", "0001"),
            (150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000", "0002"),
            (150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000", "0003")
           ]

    bf= spark.createDataFrame(data, schema)
    display(bf)
    return bf

createDataFrame2()


# COMMAND ----------

#camel to snake conversion

def camelTosnake(df):
    new_cols = [col[0].lower() + ''.join(['_'+c.lower() if c.isupper() else c for c in col[1:]]) for col in df.columns]
    bf2 = df.toDF(*new_cols)
    display(bf2)
    return(bf2)

camelTosnake(bf)


# COMMAND ----------

#convert timestamp to unix time
from pyspark.sql.functions import to_timestamp, unix_timestamp

def timestamp_to_unix(df,new_column):
    df= df.withColumn(new_column, to_timestamp(new_column, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    bf4 = df.withColumn(new_column, unix_timestamp(new_column)*1000)
    display(bf4)
    return bf4

timestamp_to_unix(bf3,"start_time")

# COMMAND ----------

#joining two dataframes
def join_two_df(df,df2):
    df = df.withColumnRenamed("product _number", "Product Number")
    joined_df = df2.join(df, "Product Number", "inner")
    display(joined_df)
    return joined_df

join_two_df(bf4,df5)


# COMMAND ----------

#to filter with a criteria
def filtering(df,criteria):
    joined1_df = df.filter(criteria)
    display(joined1_df)
    return joined1_df
    
filtering(joined_df,"language = 'EN'")



# COMMAND ----------


