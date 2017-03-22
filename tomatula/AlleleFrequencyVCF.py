## Imports
from pyspark import SparkConf, SparkContext
import sys
import argparse
import csv

## Constraints
APP_NAME = "Allele Frequencies"

## Functions

def allele_freq(elements):
    """ parses the info field of a line and returns the
        allele frequency with the position and the chromosome
    """
    chrom = elements[0]
    pos = elements[1]
    ref = elements[3]
    alt = elements[4]
    info = elements[7]
    infos = info.split(';')

    # Find the AF information in the INFO field
    AF_inf = str()
    for i in infos:
        if i[:2] == 'AF':
            AF_inf = i
            break

    if AF_inf != '':
        allele_freq = (str(chrom), str(pos), str(ref), str(alt), str(AF_inf))
        return allele_freq
    else:
        return None

## Main function
def main(sc, inputFile, outputFile, chrom, lower, upper):
    frequencies = sc.textFile(inputFile)\
                    .map(lambda line: line.split('\t'))\
                    .filter(lambda line: len(line)>1)\
                    .filter(lambda line: line[1]>=lower and line[1]<=upper)\
                    .map(allele_freq)\
                    .filter(lambda l: l!=None)
    # Save output in single csv file
    r = frequencies.collect()                
    with open(outputFile, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(["Chromosome","Position","Reference","Alternative","Allele Frequencies"])
        writer.writerows(r)
    # Alternativele save output as txt with PySpark method. 
    # This will save the output in parts. 
    # frequencies.saveAsTextFile(outputFile)

if __name__ == "__main__":
    """Query to return the allele frequency of all sites of a specific
       chromosome or region
       Input: the chromosome number and/or the start and end
       position of the region of interest
       Output: A list of the allele frequencies for all the
       respective sites, including the position and the chromosome
    """

    # Configure options
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)

    # Parse input arguments
    parser = argparse.ArgumentParser(description='Find allele frequency of selected chromosomal region using the VCF file. Store output as csv file.')
    parser.add_argument('--input', help='Input file, VCF format')
    parser.add_argument('--output', help='Name of file to store output', default='VCFoutput.csv')
    parser.add_argument('--chr', help='Chromosome of interest (default is 6)', default='6')
    parser.add_argument('--from', dest='from_', type=int, help='Starting position of region of interest')
    parser.add_argument('--to', type=int, help='End of region of interest')

    args = parser.parse_args()

    main(sc, args.input, args.output, args.chr, str(args.from_), str(args.to))



