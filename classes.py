from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import struct, col, explode, concat, concat_ws, rank
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from datetime import datetime
from pyspark.sql.window import Window

current_dateTime = datetime.now()

spark = SparkSession \
            .builder \
            .appName("pipeline") \
            .getOrCreate()


class EventProcessor:
    def init():
        self.path = path


    def read_json(self, path: str)-> DataFrame:
        """Read the json in the file path and transform it in a spark dataframe
        
        Parameters:
            path (str): The string which is the file path to json.
        
        Returns:
            df (DataFrame): The dataframe from json file.
        """
        spark_df = spark.read.option("multiline","true").json(path)

        df_flattened = spark_df.withColumn("searchItem", explode(col("data.searchItemsList")))
        
        df_flattened = df_flattened.select(
            col("id"),
            col("timestamp"),
            col("live"),
            col("organic"),
            col("data.clientId").alias("clientId"),
            col("data.platform").alias("platform"),
            col("searchItem.travelPrice").cast(DoubleType()).alias("travelPrice"),
            col("searchItem.travelCompanyId").alias("travelCompanyId"),
            col("searchItem.travelCompanyName").alias("travelCompanyName"),
            col("searchItem.distributorIdentifier").alias("distributorIdentifier"),
            col("searchItem.departureDate").alias("departureDate"),
            col("searchItem.departureHour").alias("departureHour"),
            col("searchItem.arrivalDate").alias("arrivalDate"),
            col("searchItem.arrivalHour").alias("arrivalHour"),
            col("searchItem.originId").alias("originId"),
            col("searchItem.originCity").alias("originCity"),
            col("searchItem.originState").alias("originState"),
            col("searchItem.destinationId").alias("destinationId"),
            col("searchItem.destinationCity").alias("destinationCity"),
            col("searchItem.destinationState").alias("destinationState"),
            col("searchItem.serviceClass").alias("serviceClass"),
            col("searchItem.serviceCode").alias("serviceCode"),
            col("searchItem.availableSeats").alias("availableSeats"),
            col("searchItem.price").cast(DoubleType()).alias("price"),
            col("searchItem.referencePrice").cast(DoubleType()).alias("referencePrice"),
            col("searchItem.originalPrice").cast(DoubleType()).alias("originalPrice"),
            col("searchItem.discountPercentageApplied").cast(DoubleType()).alias("discountPercentageApplied"),
            col("searchItem.tripId").alias("tripId"),
            col("searchItem.groupId").alias("groupId")
        )

        return df_flattened


    def create_columns(self, df: DataFrame)-> DataFrame:
        """Add columns departure_datetime, arrival_datetime and route to dataframe.
        
        Parameters:
            df (DataFrame): The initial dataframe.
        
        Returns:
            df (DataFrame): The dataframe with the new columns.
        """
        df = df.withColumn("departure_datetime", concat_ws(" ", df.departureDate, df.departureHour))
        df = df.withColumn("arrival_datetime", concat_ws(" ", df.arrivalDate, df.arrivalHour))
        df = df.withColumn("route", concat_ws(" ", df.originCity, df.destinationCity))

        return df


    def filter(self, df: DataFrame)-> DataFrame:
        """Filter the rows from the dataframe given by the future deapartures and available seats.
        
        Parameters:
            df (DataFrame): The initial dataframe.
        
        Returns:
            df (DataFrame): The filtered dataframe.
        """
        df = df.filter(df.departure_datetime > current_dateTime)
        df = df.filter(df.availableSeats > 0)
        
        return df


    def process_events(self, path: str)-> DataFrame:
        """Read the json in the path, normalize the data and return the proccessed dataframe. Using the methods read_json, create_columns and filter.
        
        Parameters:
            path (str): The string which is the file path to json.
        
        Returns:
            df (DataFrame): The dataframe from json file.
        """
        df = self.read_json(path)
        df = self.create_columns(df)
        df = self.filter(df)
        return df


class Aggregator:
    def avg_price_per_route_and_class(self, df: DataFrame)-> DataFrame:
        """Calculate the average price by route and class of service.
        
        Parameters:
            df (DataFrame): The full dataframe.
        
        Returns:
            df (DataFrame): The dataframe with average price by route and service.
        """
        df = df.groupBy("route", "serviceClass").avg("price")

        return df


    def sum_seat_aviable_per_route_and_company(self, df: DataFrame)-> DataFrame:
        """Determine the total available seats by route and company.
        
        Parameters:
            df (DataFrame): The full dataframe.
        
        Returns:
            df (DataFrame): The dataframe with total available seats by route and company.
        """
        df = df.groupBy("route", "travelCompanyName").sum("availableSeats")

        return df


    def most_popular_route_per_company(self, df: DataFrame)-> DataFrame:
        """Identify the most popular route by travel company.
        
        Parameters:
            df (DataFrame): The full dataframe.
        
        Returns:
            df (DataFrame): The dataframe with the most popular route by company.
        """
        df = df.groupBy("route", "travelCompanyName").sum("availableSeats")

        windowSpec = Window.partitionBy("travelCompanyName").orderBy(col("sum(availableSeats)").asc())

        df_ranked = df.withColumn("rank", rank().over(windowSpec))

        df_most_popular_routes = df_ranked.filter(col("rank") == 1).drop("rank", "sum(availableSeats)")

        return df_most_popular_routes


    def aggregate_data(self, df: DataFrame)-> DataFrame:
        """Receive the processed DataFrame, generate the requested aggregations and return dataFrames with the insights.
        
        Parameters:
            df (DataFrame): The full dataframe.
        
        Returns:
            df_avg (DataFrame): The dataframe with average price by route and service.
            df_sum (DataFrame): The dataframe with total available seats by route and company.
            df_pop (DataFrame): The dataframe with the most popular route by company.
        """
        df_avg = self.avg_price_per_route_and_class(df)
        df_sum = self.sum_seat_aviable_per_route_and_company(df)
        df_pop = self.most_popular_route_per_company(df)

        return df_avg, df_sum, df_pop


class Writer:
    def write_data(self, name: str, df: DataFrame)-> None:
        """Save data in parquet format partitioned by originState and destinationState.

        Parameters:
            df (DataFrame): The dataframe to be saved.
            name (str): The name of folder inside results to store the parquet file.
        
        Returns:
            None
        """
        if "destinationState" in df.columns and "originState" in df.columns:
            df.write.mode("overwrite").partitionBy("originState", "destinationState").parquet(f"results/{name}/")
        else:
            df.write.mode("overwrite").parquet(f"results/{name}/")
 

def main():
    event = EventProcessor()
    df = event.process_events("input_data.json")
    df.show()

    agg = Aggregator()
    df_avg, df_sum, df_pop = agg.aggregate_data(df)
    df_avg.show()
    df_sum.show()
    df_pop.show()

    writer = Writer()
    writer.write_data("general", df)
    writer.write_data("avg", df_avg)
    writer.write_data("sum", df_sum)
    writer.write_data("pop", df_pop)


if __name__ == "__main__":
    main()
