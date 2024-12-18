from pyspark.sql import SparkSession


spark = SparkSession \
            .builder \
            .appName("example") \
            .getOrCreate()


class EventProcessor:
    def init():
        self.path = path
        
    def read_json(self, path: str):
        spark_df = spark.read.option("multiline","true").json(path)
        return spark_df

    def read_array():
        pass

    def create_columns():
        #departure_time():
        #""" Combinação de departureDate e departureHour."""
        #pass
    
        #arrival_datetime():
        #""" Combinação de arrivalDate e arrivalHour."""
        #pass

        #route: Combinação originCity e destinationCity.
        pass
    
    def filter():
        #viagens futuras
        #viagens com availableSeats > 0
        pass
    
    def process_events():
        " Leia o JSON, Normalize os dados and Retorne o DataFrame processado."
        pass


class Aggregator:
    def init():
        pass

    def avg_price_per_route_and_class():
        """Calcular o preço médio por rota e classe de serviço."""
        pass

    def sum_seat_aviable_per_route_and_company():
        """Determinar o total de assentos disponíveis por rota e companhia."""
        pass

    def most_popular_route_per_company():
        """Identificar a rota mais popular por companhia de viagem."""
        pass

    def aggregate_data():
        """Receba o DataFrame processado, Gere as agregações solicitadas e Retorne um DataFrame com os insights."""
        pass


class Writer:
    def init():
        pass

    def write_data():
        """Save data in parquet format partitioned by originState and destinationState."""
        pass

def main():
    # Create an instance of the EventProcessor with a specific event name
    event = EventProcessor()
    df = event.read_json("input_data2.json")
    df.show()
    # Call methods on the event processor instance
    # event.process_event()
    # event.log_event()

# Ensure the main function runs when the script is executed
if __name__ == "__main__":
    main()
