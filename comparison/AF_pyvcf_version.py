# Test pyvcf to find frequency of alternative alleles for each individual
from __future__ import division
import vcf
import time

vcf_reader = vcf.Reader(open('/Users/katerina1/tomatula/comparison/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf', 'r'))

AF = dict()
start_pos = 1891000
stop_pos = 1892000

start_time = time.time()

# Find Allele Frequency for positions in selected region
for record in vcf_reader:
    if (record.POS >= start_pos) & (record.POS <= stop_pos):
        AF[record.POS] = record.INFO['AF']
    elif record.POS > stop_pos:
        break

print("--- %s seconds ---" % (time.time() - start_time))