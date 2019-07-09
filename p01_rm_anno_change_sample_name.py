"""
remove annotation and change sample name
"""

import glob, os
import argparse
import pandas as pd
import gzip


data_path = '/hpc/grid/hgcb/workspace/projects/P049_Schizophrenia_Sequencing/data'
gemini_path = '/hpc/grid/hgcb/workspace/users/shangzhong/GEMINI'
reformat_py = '/home/lis262/Code/Pedigree/m01_reformat.py'

# 1. find samples that are sequenced
fq_path = data_path + '/fastq/MY1610072_R1/fastq_Samples_1-50'
fq_path1 = data_path + '/fastq/MY1610072_R1/fastq_Samples_51-61'

fq_files = glob.glob(fq_path + '/*_R1_*.fastq.gz') + glob.glob(fq_path1 + '/*_R1_*.fastq.gz')
samples = ['PS-' + f.split('/')[-1].split('_')[0] for f in fq_files]

# 2. build dictionary {sample: family_personID}
def get_dict(covari_fn, samples):
	covari_df = pd.read_csv(covari_fn)
	covari_df = covari_df[covari_df['PS#'].isin(samples)]
	covari_df['NInd'] = covari_df['NInd'].astype('str')
	covari_df['Mom'] = covari_df['Mom'].astype('str')
	covari_df['Dad'] = covari_df['Dad'].astype('str')
	# change the id of the individuales
	covari_df['NInd'] = covari_df['Pedi#'] + '_' + covari_df['NInd']
	covari_df['Mom'] = covari_df['Pedi#'] + '_' + covari_df['Mom']
	covari_df['Dad'] = covari_df['Pedi#'] + '_' + covari_df['Dad']
	# build dictionary
	sample_id_dict = covari_df.set_index('PS#')['NInd'].to_dict()
	return sample_id_dict

covari_fn = data_path + '/scz_phe_covar.csv'
sample_id_dict = get_dict(covari_fn, samples)

# 3. change name and replace annotation column with . in vcf file
raw_vcf_path = data_path + '/vcf'
raw_vcf_files = glob.glob(raw_vcf_path + '/15*.vcf.gz')


gemini_vcf_path = gemini_path + '/f01_vcf_1'
if not os.path.exists(gemini_vcf_path):
	os.mkdir(gemini_vcf_path)


def reformat_vcf(in_vcf, out_vcf):
    with gzip.open(in_vcf, 'rb') as f_in, open(out_vcf,'wb') as f_out:
        for line in f_in:
            ad_pattern = b'##FORMAT=<ID=AD,Number=.'
            if line.startswith(ad_pattern):
                line = ad_pattern[:-1]+b'R'+line[len(ad_pattern):]
                f_out.write(line)
            elif line.startswith(b'#CHROM'):
                # change sample name
                item = line.strip().split(b'\t')
                changes = item[9:]
                item[9:] = [sample_id_dict['PS-' + i.decode('utf-8')].encode() for i in changes]
                f_out.write(b'\t'.join(item) + b'\n')
            elif line.startswith(b'#'):
                f_out.write(line)
            else:
                # remove INFO filed
                item = line.strip().split(b'\t')
                item[7] = b'.'
                f_out.write(b'\t'.join(item) + b'\n')

    os.system('bgzip {f}'.format(f=out_vcf))
    os.system('tabix {f}.gz'.format(f=out_vcf))


for vcf in raw_vcf_files:
    base_name = os.path.basename(vcf)
    index = base_name.index('_S1')
    sample_id = 'PS-' + os.path.basename(vcf)[:index]
    out = gemini_vcf_path + '/' + sample_id_dict[sample_id] + '.vcf'
    reformat_vcf(vcf, out)

    # submit jobs
    # cmd = 'bsub -o log.txt -e log.err.txt \"python {reformat} -i {input} -o {output}\"'.format(reformat=reformat_py, input=vcf,output=out)
    # os.system(cmd)

    
    