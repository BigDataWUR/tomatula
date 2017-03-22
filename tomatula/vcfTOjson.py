## Imports
from pyspark import SparkConf, SparkContext
from itertools import groupby
import argparse

## Constraints
APP_NAME = "Convert vcf to json"

## Additional function
def lineTodict(line, header, individuals):
    d = dict()

    fields = line.split('\t')

    for i in range(len(fields)):
        if fields[i].isdigit():
            d[str(header[i])] = int(fields[i])
        else:
	    d[str(header[i])] = str(fields[i])

    # convert info field in an embeded dictionary
    s = d['INFO']

    info = s.split(';')
    info_dict = dict()
    for i in info:
        l = i.split('=')

	if l[1].isdigit():
	    info_dict[str(l[0])] = int(l[1])
	else:
	    info_dict[str(l[0])] = str(l[1])

    del d['INFO']

    d['INFO'] = info_dict

    # split individuals' information into embeded dictionaries

    f = d['FORMAT'].split(':')

    for key in d:
        if key in individuals:
            ind_dict = dict()
            form = d[key].split(':')

            if len(form) > 1:
                for i in range(len(f)):
		    if form[i].isdigit():
			ind_dict[str(f[i])] = int(form[i])
		    else:
			ind_dict[str(f[i])] = str(form[i])

            del d[key]

            d[str(key)] = ind_dict
    return d

## Main Function

def main(sc, inputFile, outputFile):

    # Load the file
    lines = sc.textFile(inputFile)

    # Define headers
    headerLines = lines.filter(lambda line: line.startswith(chr(35)))\
                       .filter(lambda line: line[1] != chr(35))\
                       .map(lambda line: line[1:])\
                       .map(lambda line: line.split("\t"))\
                       .collect()

    header = []
    for i in headerLines[0]:
        if i[0] == '/':
	    h = i[1:]
	else:
	    h = i
	h = h.replace('.', '_').replace('/', '_').replace('-', '_')
	header.append(h)

    individuals = header[9:]

    # Convert VCF data lines to json dictionary and store in file
    lines.filter(lambda line: not line.startswith(chr(35)))\
         .map(lambda line: lineTodict(line, header, individuals))\
         .saveAsTextFile(outputFile)

if __name__ == "__main__":

    # Configure Options
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)

    # Parse arguments
    parser = argparse.ArgumentParser(description='Convert vcf file to json object')
    parser.add_argument('--input', help='Input file, VCF format')
    parser.add_argument('--output', help='Name (and directory) of file to store output')

    args = parser.parse_args()

    # Execute main function
    main(sc, args.input, args.output)
