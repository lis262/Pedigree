import glob, os

path = '/hpc/grid/hgcb/workspace/users/shangzhong/GEMINI'

vcf_path = path + '/f01_vcf'
vcfs = glob.glob(vcf_path + '/*.vcf.gz')
families = []
for vcf in vcfs:
	base = vcf.split('/')[-1]
	index = base.index('_')
	families.append(base[:index])
families = list(set(families))
# print(families)
for f in families:
	os.system('python ~/Code/Pedigree/p02_merge_norm_vcf.py -f {f}'.format(f=f))
