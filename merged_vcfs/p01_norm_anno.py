import os
import argparse

parser = argparse.ArgumentParser(description='normalize and annotate vcf file')

parser.add_argument('-c','--cores',action='store',type=int,dest='cores',help='number of cores',default=4)
parser.add_argument('-v','--vcf',action='store',dest='vcf',help='cohort vcf file')
parser.add_argument('-g','--ref',action='store',dest='ref',help='reference genome',default='/hpc/grid/hgcb/workspace/users/shangzhong/GATK/genome/hg38.fasta')
parser.add_argument('-a','--vep',action='store',dest='vep',help='path to vep database',default='/hpc/grid/hgcb/workspace/users/shangzhong/publicDB/vep_db_96')
parser.add_argument('-p','--ped',action='store',dest='ped',help='pedigree file',default='/hpc/grid/hgcb/workspace/users/shangzhong/SCZ/pedigree.ped')

args = parser.parse_args()
cores = args.cores
vcf = args.vcf
ref = args.ref
vep_db = args.vep
ped_fn = args.ped
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


norm_vcf = vcf.split('.')[0] + '.norm.vcf.gz'
normalize(ref, vcf, norm_vcf)

#===================== annotate vcf file =================
def annotate_VEP(in_vcf, out_vcf, vep_db):
    cmd = ('vep -i {vcf} '
	      '-o {out} --vcf --compress_output bgzip --cache '
	      '--dir {db} --sift b --polyphen b '
	      '--symbol --numbers --biotype '
	      '--total_length --fields '
	      'Consequence,Codons,Amino_acids,Gene,'
	      'SYMBOL,Feature,EXON,PolyPhen,SIFT,'
	      'Protein_position,BIOTYPE --offline --fork 4 --force_overwrite').format(
	      	vcf=in_vcf, out=out_vcf, db=vep_db)
    os.system(cmd)
    cmd = 'tabix {vcf}'.format(vcf=out_vcf)
    os.system(cmd)


anno_vcf = vcf.split('.')[0] + '.norm.vep.vcf.gz'
annotate_VEP(norm_vcf, anno_vcf, vep_db)

