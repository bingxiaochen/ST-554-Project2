from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:

    def __init__(self, df: DataFrames):
        self.df = df

    @classmethod
    def with_grades(self, name: str, unity: str, grades: list):
        student = self(name, unity)
        student.grades = grades
        return student

class SparkDataCheck:

    def __init__(self, df):
        self.df = df

    @classmethod
    def from_csv(class, spark, path):
        """
        Create an instance by reading a CSV file

        """
        df = spark.read.load(
            path,
            format="csv",
            header=True,
            inferSchema=True
        )
        return class(df)

    @classmethod
    def from_pd(class, spark, df):
        """
        Create an instance from a pandas DataFrame

        """
        df = spark.createDataFrame(pandas_df)
        return class(df)
