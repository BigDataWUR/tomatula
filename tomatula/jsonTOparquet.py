## Imports
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
import argparse

## Constraints
APP_NAME = "Infer schema and save json file in Parquet format"

def main(sc, inputFile, outputFile):

    # Load the file and infer the schema
    variants = sqlContext.read.json(inputFile)

    # The inferred schema can be visualized using printSchema() method
    # variants.printSchema()

    # Save the DataFrame as a Parquet file, maintaining the schema information
    variants.write.parquet(outputFile)

if __name__ == "__main__":

    # Configure Options
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Parse arguments
    parser = argparse.ArgumentParser(description='Infer schema and save JSON file in Parquet format')
    parser.add_argument('--input', help='Input file, JSON format')
    parser.add_argument('--output', help='Name (and directory) of file to store output')

    args = parser.parse_args()

    # Execute main function
    main(sc, args.input, args.output)

