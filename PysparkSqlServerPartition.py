
# Create a Spark session
spark = SparkSession.builder.appName("PySpark SQL Server Example").getOrCreate()

# Define the JDBC URL and credentials
url = "jdbc:sqlserver://<server>:<port>;databaseName=<database>"
user = "<user>"
password = "<password>"

# Read the table from SQL Server
df = spark.read.format("jdbc").option("url", url).option("user", user).option("password", password).option("dbtable", "<table>").load()

# Repartition the dataframe by a column
df_repartitioned = df.repartition(100, "id")

# Query the dataframe from a specific partition
df_partitioned = df_repartitioned.filter(spark_partition_id() == 1)





