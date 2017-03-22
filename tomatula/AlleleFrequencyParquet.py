## Imports
from pyspark.sql import SQLContext, Row
from pyspark import SparkConf, SparkContext
import sys
import argparse

## Constraints
APP_NAME = "Allele Frequency Parquet Version"

## Main function
def main(sc, inputFile, outputFile, chrom, lower, upper):
    # Load Parquet File as DataFrame
    Variant_Calls = sqlContext.read.parquet(inputFile).cache()
    #perform querying on the DataFrame
    r = Variant_Calls.select("CHROM","POS","REF","ALT","INFO.AF")\
                     .filter("POS > " + lower + " AND POS < " + upper)
    # Save output in csv format
    # use format="json" to save as JSON and refer to Spark 
    # documentation for other available output formats
    r.write.save(outputFile, format="csv")

if __name__ == "__main__":
    """Read from the parquet table the rows that correspond to the region
       of interest and return the frequency of each allele in each position
    """
    # Configure options
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Parse inputs
    parser = argparse.ArgumentParser(description="Find allele frequency of selected chromosomal region using the Parquet format. Store output as csv file.")
    parser.add_argument("--input", required = True, help="Input file/directory.")
    parser.add_argument("--output", help="Name of file to store output.", default="ParqOutput.csv")
    parser.add_argument("--chr", help="Chromosome of interest (default is 6)", default="6")
    parser.add_argument("--from", dest="From", required = True, type=str, help="Starting position of region of interest")
    parser.add_argument("--to", required = True, type=str, help="End of region of interest")

    args = parser.parse_args()

    # Execute main function
    main(sc, args.input, args.output, args.chr, args.From, args.to)
