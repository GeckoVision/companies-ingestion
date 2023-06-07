from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import glob
import unidecode
from pyspark.sql import DataFrame
from jobs.jobBase import JobBase


class DataframeService(JobBase):
    def __init__(self, spark):
        super().__init__()
        DataFrame.transform = self.transform
        self.spark = spark

    def renameColumns(self, schema, df):
        df = df.select([col(c).alias(schema.get(unidecode.unidecode(c), unidecode.unidecode(c))) for c in df.columns])
        return df.select([F.col(c).cast('string') for c in schema.values()])

    def read_csv(self, path, delimiter, encoding="ISO-8859-1"):
        """
            This method is responsible to read the csv and return
            the builded df
            :param encoding:
            :param path: the input path of the dataset
            :param delimiter: the delimiter of the dataset
            :return: df - the df with the contents you will read
        """

        df = self.spark.read.option("delimiter", delimiter).option("header", False).option("encoding", encoding).csv(path)
        return df

    def define_dfs(self, schema, file, delimiter, encoding="ISO-8859-1"):
        """
            This method is responsible to create the df name base on the list provided and define the df
            :param encoding:
            :param schema: the schema of the dataset
            :param file: the list of files to be read in the input directory
            :param delimiter: the delimiter that will be used to parse your file
        """
        return self.renameColumns(schema, self.read_csv(file, delimiter=delimiter, encoding=encoding))
