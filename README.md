# tomatula

Tomatula is an application that uses [Apache Hadoop](http://hadoop.apache.org) 
and [Apache Spark](http://spark.apache.org) to store and query big Variant 
Calling datasets. It introduces a columnar storage format instead of the
flat VCF format for storing the Variant Calling data and storing them across 
the Hadoop Distributed File System.

## Parquet-based data storage format for VCF

This repository contains the scripts to convert a VCF file to the [Apache 
Parquet](http://parquet.apache.org/) format. The conversion consists of two 
steps: 

In the first step, the VCF file is
parsed and stored as a JSON object. The field names are used as keys of the 
JSON dictionary and the INFO and individuals' fields are further split into 
embedded dictionaries. 
The FORMAT field is used to define the keys of the 
embedded dictionaries of each individuals' field and is not stored as a separate
value in the JSON and parquet formats.

In the second step the JSON object is used to create a new [Spark Dataframe](https://spark.apache.org/docs/2.0.0/sql-programming-guide.html#dataframes) and 
store the data under the new columnar parquet format. Apache Spark includes a 
built-in    library for loading a JSON object an automatically detecting the 
schema. 
The keys of the JSON dictionary are used as the field names and the embedded 
dictionaries are stored as bunches of pages under each field.

The following is a part of the Schema used to store variant calling data (only 
one individual's schema is shown here).


```
|-- CHROM: long (nullable = true)
|-- POS: long (nullable = true)
|-- ID: string (nullable = true)
|-- REF: string (nullable = true)
|-- ALT: string (nullable = true) 
|-- QUAL: string (nullable = true)
|-- FILTER: string (nullable = true)
|-- INFO: struct (nullable = true)
|    |-- AB: string (nullable = true)
|    |-- ABP: string (nullable = true)
|    |-- AC: string (nullable = true)
|    |-- AF: string (nullable = true)
|    |-- AN: string (nullable = true)
|    |-- ANN: string (nullable = true)
|    |-- AO: string (nullable = true)
|    |-- CIGAR: string (nullable = true)
|    |-- DP: string (nullable = true)
|    |-- DPB: string (nullable = true)
|    |-- DPRA: string (nullable = true)
|    |-- EPP: string (nullable = true)
|    |-- EPPR: string (nullable = true)
|    |-- GTI: string (nullable = true)
|    |-- LEN: string (nullable = true)
|    |-- LOF: string (nullable = true)
|    |-- MEANALT: string (nullable = true)
|    |-- MQM: string (nullable = true)
|    |-- MQMR: string (nullable = true)
|    |-- NMD: string (nullable = true)
|    |-- NS: string (nullable = true)
|    |-- NUMALT: string (nullable = true)
|    |-- ODDS: string (nullable = true)
|    |-- PAIRED: string (nullable = true)
|    |-- PAIREDR: string (nullable = true)
|-- LYC2740: struct (nullable = true)
|    |-- AO: string (nullable = true)
|    |-- DP: string (nullable = true)
|    |-- GL: string (nullable = true)
|    |-- GT: string (nullable = true)
|    |-- QA: string (nullable = true)
|    |-- QR: string (nullable = true)
|    |-- RO: string (nullable = true)
```


The repository contains an example dataset of variant calling data of different 
types of the tomato clade in both VCF and Parquet format. The example of the 
genomes ranges within positions 40000000 and 40004000 of chromosome 6 of the 
tomato genome (from [Aflitos et al. 2014](http://dx.doi.org/10.1111/tpj.12616)).

## Installation

In order to run the examples you need to have Spark installed. You can find 
instructions for building Spark at the [official Apache Spark 
website](http://spark.apache.org/docs/latest/building-spark.html).

## Example query

Assuming that you have installed Spark and `$SPARK_HOME` is set in your 
environment, you can clone this repository with

```
git clone tomatula
```

Two example queries for finding the allele frequencies within a selected range 
on the genome are included, one for each type of input file. 


```    
$SPARK_HOME/bin/spark-submit tomatula/AlleleFrequencyVCF.py --input examples/example.vcf --output outputs/outputFile1.txt --chr 6 --from 40002000 --to 40003000
```

```
$SPARK_HOME/bin/spark-submit tomatula/AlleleFrequencyParquet.py --input examples/example.parquet --output outputFile2.txt --chr 6 --from 40002000 --to 40003000
```



For the conversion from VCF to parquet via JSON try the following code:

```
$SPARK_HOME/bin/spark-submit tomatula/vcfTOjson.py --input examples/example.vcf --output outputs/example.json

$SPARK_HOME/bin/spark-submit tomatula/jsonTOparquet.py --input outputs/example.json --output outputs/example.parquet
```

**Note 1**  
In case you are using a remote cluster, depending on the configuration, scripts may need to be called with absolute paths, as in:

```    
$SPARK_HOME/bin/spark-submit tomatula/AlleleFrequencyVCF.py --input file://$(pwd)/examples/example.vcf --output file://$(pwd)/outputs/outputFile1.txt --chr 6 --from 40002000 --to 40003000
```

**Note 2**  
You can specify the number of executors to be used for the application by adding ` --num-executors`. 

**Note 3**
To allow for saving in CSV fromat, in Spark versions lesser than 2.0 the optional argument of `spark-submit` may be called:
`--packages com.databricks:spark-csv_2.10:1.4.0`, as in

```    
$SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.4.0 tomatula/AlleleFrequencyVCF.py --input file://$(pwd)/examples/example.vcf --output file://$(pwd)/outputs/outputFile1.txt --chr 6 --from 40002000 --to 40003000
```

## Attribution and feedback
Please send your feedback to A. Boufea or I.N. Athanasiadis.

Cite this work as: 
> A. Boufea, R. Finkers, M. van Kaauwen, M. Kramer and  I.N. Athanasiadis, [Managing Variant Calling Files the Big Data Way: Using HDFS and Apache Parquet](https://doi.org/10.1145/3148055.3148060), Proceedings of the Fourth IEEE/ACM International Conference on Big Data Computing, Applications and Technologies (BDCAT '17). ACM, 2017, p. 219-226, [doi:10.1145/3148055.3148060](https://doi.org/10.1145/3148055.3148060)

Experimental data have been deposited on Zenodo [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.582145.svg)](https://doi.org/10.5281/zenodo.582145)


