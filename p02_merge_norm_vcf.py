import glob, os
import argparse

parser = argparse.ArgumentParser(description='preprose vcf files for GEMINI')

parser.add_argument('-f','--family',action='store',dest='family',help='family name',default='all')
args = parser.parse_args()

family = args.family
#================== merge vcf files ===============
path = '/hpc/grid/hgcb/workspace/users/shangzhong/GEMINI'

def merge_vcf_files(vcfs, merge_vcf):
	'''
	* vcfs: input vcf files to merge
	* merge_vcf: output
	'''
	cmd = 'bcftools merge --merge all -o {out} -O z {in_vcf}'.format(out=merge_vcf,in_vcf=' '.join(vcfs))

	os.system(cmd)
	os.system('tabix {merge}'.format(merge=merge_vcf))

#===================== normalize vcf file ================

def normalize(ref, merge_vcf, norm_vcf):
	'''
	* ref: reference fa file
	* merge_vcf: merged vcf file
	* norm_vcf: output normalized vcf 
	'''
	cmd = 'vt decompose -s {merge_vcf} | vt normalize -r {ref} -o {norm_vcf} - '.format(merge_vcf=merge_vcf, norm_vcf=norm_vcf, ref=ref)
	os.system(cmd)
	os.system('tabix {norm}'.format(norm=norm_vcf))

vcf_path = path + '/f01_vcf'
merge_vcf_path = path + '/f02_merge_vcf'
if not os.path.exists(merge_vcf_path):
	os.mkdir(merge_vcf_path)

norm_path = path + '/f03_norm_vcf'
if not os.path.exists(norm_path):
	os.mkdir(norm_path)

merge_vcf = merge_vcf_path + '/' + family + '.vcf.gz'
norm_vcf = norm_path + '/' + family + '_norm.vcf.gz'
ref = path + '/hg19.fa'

if family == 'all':
	vcfs = [v for v in glob.glob(vcf_path + '/*.vcf.gz')]
else:
	vcfs = [v for v in glob.glob(vcf_path + '/*.vcf.gz') if family in v]

merge_vcf_files(vcfs, merge_vcf)
normalize(ref, merge_vcf, norm_vcf)