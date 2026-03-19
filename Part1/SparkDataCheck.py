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

    # Check if each value in a string column falls within specified levels
    def within_levels(self, col_name, levels):
    """
    Check whether values in a string column fall within a set of levels.

    """

    # get dtypes
    dict = dict(self.df.dtypes)

    if dict[col_name] != "string":
        print(f"Column '{col_name}' is not a string column.")
        return self

    # Default new column name
    new_col = f"{col_name}_in_levels"

    col = F.col(col_name)

    # check if the values are in levels
    in_level = col.isin(levels)

    # For any NULL, return NULL
    result_col = F.when(col.isNull(), None).otherwise(in_level)

    # Append column
    self.df = self.df.withColumn(new_col, result_col)

    return self


    # Check if each value in a given column is missing
    def is_missing(self, col_name):
    """
    Check whether values in a column is missing.

    """

    # Default new column name
    new_col = f"{col_name}_is_missing"

    col = F.col(col_name)

    # check if the values is missing
    result_col = col.isNull()

    # Append column
    self.df = self.df.withColumn(new_col, result_col)

    return self


    # Report the min and max of a given numeric column
    # allow one optional grouping var
    def min_max(self, col_name=None, group_by=None):
    """
    Report min and max for numeric columns.

    """

    # check column type
    dict = dict(self.df.dtypes)
    numeric_types = {"int", "bigint", "double", "float", "longint", "integer"}

    # If a numeric column in supplied, then compute the min and max
    # for this column, grouped if appropriate
    if col_name is not None:

        # Check if the column is numeric
        if dict[col_name] not in numeric_types:
            print(f"Column '{col_name}' is not numeric. Please give a numeric column.")
            return None

        if group_by is not None:
            result = (
                self.df
                .groupBy(group_by)
                .agg(
                    F.min(col_name).alias(f"{col_name}_min"),
                    F.max(col_name).alias(f"{col_name}_max")
                )
            )
        else:
            result = (
                self.df
                .agg(
                    F.min(col_name).alias(f"{col_name}_min"),
                    F.max(col_name).alias(f"{col_name}_max")
                )
            )

        return result


    # If no column is supplided, return min and max for all numeric columns
    if col_name is None:

        numeric_cols = [c for c, t in dict.items() if t in numeric_types]

        # if a grouping var is provided:
        if group_by:
            dfs = []
            for col in numeric_cols:
                temp = (
                    self.df
                    .groupBy(group_by)
                    .agg(
                        F.min(col).alias(f"{col}_min"),
                        F.max(col).alias(f"{col}_max")
                    )
                )
                dfs.append(temp)

            # Merge all results
            result = reduce(lambda df1, df2: df1.join(df2, on=group_by), dfs)

        else:
            # if no grouping var provided, just report the min and max
            agg_report = []
            for col in numeric_cols:
                agg_report.append(F.min(col).alias(f"{col}_min"))
                agg_report.append(F.max(col).alias(f"{col}_max"))

            result = self.df.agg(*agg_report)

        return result
