# Test pyvcf to find frequency of alternative alleles for each individual
from __future__ import division
import vcf

vcf_reader = vcf.Reader(open('/Users/katerina1/tomatula/comparison/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf', 'r'))

# Count all alternative alleles for each individual
alt_freq = [0] * len(vcf_reader.samples)
nrecords=0

for record in vcf_reader:
    nrecords += 1
    if nrecords % 1000 == 0:
        print nrecords
    for i in range(len(record.samples)):
        sample = record.samples[i]
        alt_freq[i] += sample['GT'].count('1')

alt_freq[:] = [x/nrecords for x in alt_freq]
samples = vcf_reader.samples

with open("/Users/katerina1/Desktop/alternative_frequencies.txt", "w") as out_file:
    for i in range(len(alt_freq)):
        new_line = samples[i] + "\t" + str(round(alt_freq[i], 4)) + "\n"
        out_file.write(new_line)