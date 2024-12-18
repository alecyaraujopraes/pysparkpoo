from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("FlattenJson").getOrCreate()

# Sample Data: Load JSON data from a string or a file
json_data = """{
  "id": "747cf833-1009-4648-94f1-358d44e159aa",
  "timestamp": "2024-11-28T13:12:27.322211538",
  "data": {
    "searchItemsList": [
      {
        "travelPrice": "180.50",
        "travelCompanyId": 101,
        "travelCompanyName": "Rapido Vermelho",
        "distributorIdentifier": "rotaonline",
        "departureDate": "2024-12-19",
        "departureHour": "10:00",
        "arrivalDate": "2024-12-19",
        "arrivalHour": "18:00",
        "originId": 1,
        "originCity": "RIO DE JANEIRO",
        "originState": "RJ",
        "destinationId": 2,
        "destinationCity": "SÃO PAULO",
        "destinationState": "SP",
        "serviceClass": "EXECUTIVO",
        "serviceCode": "RJSP-100",
        "availableSeats": 12,
        "price": 180.50,
        "referencePrice": null,
        "originalPrice": 200.00,
        "discountPercentageApplied": 9.75,
        "tripId": null,
        "groupId": "RJSPS"
      },
      {
        "travelPrice": "450.00",
        "travelCompanyId": 202,
        "travelCompanyName": "VIAGEM CONFORTÁVEL",
        "distributorIdentifier": "busfácil",
        "departureDate": "2024-12-20",
        "departureHour": "22:00",
        "arrivalDate": "2024-12-21",
        "arrivalHour": "6:00",
        "originId": 3,
        "originCity": "BELO HORIZONTE",
        "originState": "MG",
        "destinationId": 4,
        "destinationCity": "BRASÍLIA",
        "destinationState": "DF",
        "serviceClass": "LEITO",
        "serviceCode": "BHBR-202",
        "availableSeats": 5,
        "price": 450.00,
        "referencePrice": null,
        "originalPrice": 480.00,
        "discountPercentageApplied": 6.25,
        "tripId": null,
        "groupId": "BHBR"
      },
      {
        "travelPrice": "95.00",
        "travelCompanyId": 303,
        "travelCompanyName": "TURISMO FÁCIL",
        "distributorIdentifier": "viajafácil",
        "departureDate": "2024-12-25",
        "departureHour": "14:00",
        "arrivalDate": "2024-12-25",
        "arrivalHour": "18:00",
        "originId": 5,
        "originCity": "CURITIBA",
        "originState": "PR",
        "destinationId": 6,
        "destinationCity": "JOINVILLE",
        "destinationState": "SC",
        "serviceClass": "CONVENCIONAL",
        "serviceCode": "CJPR-303",
        "availableSeats": 25,
        "price": 95.00,
        "referencePrice": null,
        "originalPrice": 100.00,
        "discountPercentageApplied": 5.00,
        "tripId": null,
        "groupId": "CJPR"
      }
    ],
    "platform": null,
    "clientId": null
  },
  "live": true,
  "organic": true
}"""

# Read the JSON data into a DataFrame
df = spark.read.json(spark.sparkContext.parallelize([json_data]))

# Flatten the JSON structure
# First, explode the 'searchItemsList' array to create multiple rows
df_flattened = df.withColumn("searchItem", explode(col("data.searchItemsList")))

# Now, select all top-level columns along with the exploded 'searchItem' structure
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

# Show the flattened DataFrame
df_flattened.show(truncate=False)
