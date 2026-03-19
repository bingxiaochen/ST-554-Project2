from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

# Creates the class
class SparkDataCheck:

    # create a .df attribute that is the dataframe
    def __init__(self, df):
        self.df = df

    # create spark df using csv file
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

    # create spark df from a pandas dataframe
    @classmethod
    def from_pd(class, spark, df):
        """
        Create an instance from a pandas DataFrame

        """
        df = spark.createDataFrame(pandas_df)
        return class(df)

    # create a method that check if each value is within the specified range
    def within_range(self, col_name, lower, upper):
        """
        Create a method that checks if each value in a numeric column is within
        user defined limits and returns the dataframe with an appended column of
        Boolean values.

        """

        # Check at least one bound is provided
        # So if none is provided, returns error and return the original dataframe
        if lower is None and upper is None:
            print("At least one of 'lower' or 'upper' must be provided. ")
            return self

        # Check column type
        # create a dict of column and its type
        dict = dict(self.df.dtypes)

        # create a dictionary of numeric types
        numeric_types = {"int", "bigint", "double", "float", "longint", "integer"}

        # check if the column type is one of the numeric types
        if dict[col_name] not in numeric_types:
            print(f"Column '{col_name}' is not numeric. ")
            return self

        # Create a new column to store the Boolean values
        new_col = f"{col_name}_in_range"

        col = F.col(col_name)

        # check if the column is in range
        if lower is not None and upper is not None:
            in_range = col.between(lower, upper)
        elif lower is not None:
            in_range = col >= lower
        else:
            in_range = col <= upper

        # For any NULL values, return NULL
        result_col = F.when(col.isNull(), None).otherwise(in_range)

        # Append column
        self.df = self.df.withColumn(new_col, result_col)

        return self
