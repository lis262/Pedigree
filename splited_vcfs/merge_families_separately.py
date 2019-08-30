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
	cmd = 'bsub -n 4  -R \"span[ptile=4]\" -o log.txt -e log.err.txt -q medium \"python /home/lis262/Code/Pedigree/p02_merge_norm_anno_vcf.py -f {fam}\"'.format(fam=f)
	os.system(cmd)
	