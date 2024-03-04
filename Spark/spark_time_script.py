import argparse
import time

from pyspark.sql import SparkSession

def calculate_delay_by_type(data_source, query_output_uri, time_output_uri):
    with SparkSession.builder.appName("Calculate Flight Delay").getOrCreate() as spark:
        if data_source is not None:
            delayed_flights_df = spark.read.option("header", "true").csv(data_source)

        delayed_flights_df.createOrReplaceTempView("delayed_flights")

        delay_types = ["CarrierDelay", "NASDelay", "WeatherDelay", "LateAircraftDelay", "SecurityDelay"]

        for delay_type in delay_types:
            type_of_delay = f"year_wise_{delay_type}"

            
            delay_type_query = f"""
                                SELECT 
                                    Year, avg(({delay_type} / ArrDelay) * 100) 
                                AS 
                                    {type_of_delay}
                                FROM 
                                    delayed_flights 
                                GROUP BY 
                                    Year
                                """
            delay_type_df = spark.sql(delay_type_query)
            query_result_output_path = f"{query_output_uri}/{delay_type.lower()}"
            delay_type_df.write.option("header", "true").mode("overwrite").csv(query_result_output_path)

            elapsed_times = []

            #calculating time for 5 iterations

            i = 0
            while i < 5:
                start_time = time.time()
                spark.sql(delay_type_query)
                end_time = time.time()

                elapsed_time = end_time - start_time
                elapsed_times.append(elapsed_time)
                i += 1

            elapsed_time_df = spark.createDataFrame([tuple(elapsed_times)], schema=["iteration1", "iteration2", "iteration3", "iteration4", "iteration5"])
            time_elapsed_output_path = f"{time_output_uri}/{delay_type.lower()}"
            elapsed_time_df.write.option("header", "true").mode("overwrite").csv(time_elapsed_output_path)
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for your CSV flight data, like an S3 bucket location.")
    parser.add_argument(
        '--query_output_uri', help="The base URI where output is saved, like an S3 bucket location.")
    parser.add_argument(
        '--time_output_uri', help="The base URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_delay_by_type(args.data_source, args.query_output_uri, args.time_output_uri)