import subprocess
import os, glob
from multiprocessing import  Pool
import multiprocessing

cores = multiprocessing.cpu_count()


path = '/hpc/grid/hgcb/workspace/users/shangzhong/GEMINI'
raw_vcf_path = path + '/f03_norm_vcf'
anno_vcf_path = path + '/f04_anno_vcf'
vep_db = path + '/vep_db'

if not os.path.exists(anno_vcf_path):
	os.mkdir(anno_vcf_path)

vcf_files = glob.glob(raw_vcf_path + '/*.vcf.gz')
anno_vcf_files = [anno_vcf_path+'/'+v.split('/')[-1][:-6]+'vep.vcf.gz'
				for v in vcf_files]

def annotate_VEP(in_vcf, out_vcf, vep_db):
    cmd = 'vep -i {vcf} \
	      -o {out} --vcf --cache \
	      --dir {db} --sift b --polyphen b \
	      --symbol --numbers --biotype \
	      --total_length --fields \
	      Consequence,Codons,Amino_acids,Gene,\
	      SYMBOL,Feature,EXON,PolyPhen,SIFT,\
	      Protein_position,BIOTYPE --offline --fork 4'.format(
	      	vcf=in_vcf, out=out_vcf, db=vep_db)
    subprocess.call(cmd, shell=True)
    cmd = 'tabix {vcf}'.format(vcf=out_vcf)
    subprocess.call(cmd, shell=True)


# if __name__ == '__main__':
#     p = Pool(processes=2)
#     for raw, anno in zip(vcf_files, anno_vcf_files):
#     	p.apply_async(annotate_VEP, args=(raw, anno, vep_db))
#     p.close()
#     p.join()


for raw, anno in zip(vcf_files, anno_vcf_files):
	cmd = 'bsub -n 4  -R \"span[ptile=4]\" -o log.txt -e log.err.txt -q medium /home/lis262/Code/Pedigree/VEP.sh {i} {o} {db}'.format(i=raw,o=anno,db=vep_db)
	subprocess.call(cmd, shell=True)
	# print(cmd)