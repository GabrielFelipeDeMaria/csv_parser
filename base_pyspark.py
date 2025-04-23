import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class BasePySparkV2:
    """Base Class to PySpark V2"""

    def configure_spark_for_s3(self, spark, aws_access_key, aws_secret_key, aws_region="us-east-1"):
        """
        Configures Spark session to connect to S3.
        """
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", aws_access_key)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
        hadoop_conf.set("fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.connection.maximum", "100")

    def process_parquet_from_s3(self, aws_access_key, aws_secret_key, aws_region="us-east-1"):
        """
        Reads and processes Parquet files from an S3 path using PySpark.
        """
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("S3 Parquet Processing") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.maxResultSize", "4g") \
            .getOrCreate()

        # Configure Spark for S3
        self.configure_spark_for_s3(spark, aws_access_key, aws_secret_key, aws_region)
        return spark
    
    def spark_s3(self, aws_access_key, aws_secret_key, aws_region="us-east-1"):
        self.spark = self.process_parquet_from_s3(aws_access_key, aws_secret_key, aws_region)
        return self.spark

    def filters_df(self, df_spark: pyspark.sql.DataFrame, filters: list):
        """
        Filters a DataFrame based on the provided filters.
        """

        df_spark.createOrReplaceTempView('df_spark')

        if not filters:
            return df_spark  # Retorna sem filtros se a lista estiver vazia

        filters_str = ' AND '.join(filters)

        df_ = self.spark.sql(f"SELECT * FROM df_spark WHERE {filters_str}")

        return df_
