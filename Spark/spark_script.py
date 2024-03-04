import argparse

from pyspark.sql import SparkSession

def calculate_delay_by_type(data_source, output_uri):
    with SparkSession.builder.appName("Calculate Delay by Type").getOrCreate() as spark:
        if data_source is not None:
            delayed_flights_df = spark.read.option("header", "true").csv(data_source)

        delayed_flights_df.createOrReplaceTempView("delayed_flights")

        delay_types = ["CarrierDelay", "NASDelay", "WeatherDelay", "LateAircraftDelay", "SecurityDelay"]

        for delay_type in delay_types:
            delay_type_column = f"year_wise_{delay_type}"
            delay_type_query = f"""
                                SELECT 
                                    Year, avg(({delay_type} / ArrDelay) * 100) AS {delay_type_column}
                                FROM 
                                    delayed_flights 
                                GROUP BY 
                                    Year
                                """

            delay_type_df = spark.sql(delay_type_query)

            output_path = f"{output_uri}/{delay_type.lower()}_delay"
            delay_type_df.write.option("header", "true").mode("overwrite").csv(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for your CSV flight data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The base URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_delay_by_type(args.data_source, args.output_uri)
