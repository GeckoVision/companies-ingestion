from jobs.jobIngestionProviders import DataIngestion
from pyspark.sql import SparkSession


def get_spark_session():
    spark = (
        SparkSession.builder
        .config("spark.yarn.dist.files", "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/out/prospect_ingestion.pex")
        .config("spark.jars.packages",  "org.apache.hudi:hudi-spark3.1-bundle_2.12:0.12.0")
        .getOrCreate()
    )
    return spark


def main():
    spark = get_spark_session()
    DataIngestion(spark).run()


if __name__ == "__main__":
    main()
