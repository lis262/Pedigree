import glob, os
import argparse

parser = argparse.ArgumentParser(description='preprose vcf files for GEMINI')

parser.add_argument('-f','--family',action='store',dest='family',help='family name',default='all')
parser.add_argument('-c','--cores',action='store',type=int,dest='cores',help='number of cores',default=4)

args = parser.parse_args()
family = args.family
cores = args.cores
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


vcf_path = path + '/f01_vcf'
merge_vcf_path = path + '/f02_merge_vcf'
if not os.path.exists(merge_vcf_path):
	os.mkdir(merge_vcf_path)

if family == 'all':
	vcfs = [v for v in glob.glob(vcf_path + '/*.vcf.gz')]
else:
	vcfs = [v for v in glob.glob(vcf_path + '/*.vcf.gz') if family + '_' in v]

merge_vcf = merge_vcf_path + '/' + family + '.vcf.gz'
merge_vcf_files(vcfs, merge_vcf)

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


norm_path = path + '/f03_norm_vcf'
if not os.path.exists(norm_path):
	os.mkdir(norm_path)

norm_vcf = norm_path + '/' + family + '_norm.vcf.gz'
ref = path + '/hg19.fa'
normalize(ref, merge_vcf, norm_vcf)

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

anno_path = path + '/f04_anno_vcf'
if not os.path.exists(anno_path):
	os.mkdir(anno_path)
vep_db = path + '/vep_db'
anno_vcf = anno_path + '/' + family + '_vep.vcf.gz'
annotate_VEP(norm_vcf, anno_vcf, vep_db)

#===================== load vcf into gemini ================
def load_vcf2gemini(vcf, db, ped, cores, tmp):
	cmd = 'gemini load -v {vcf} -p {ped} -t VEP --cores {cores} {db} --tempdir {tmp}'.format(vcf=vcf,ped=ped,cores=cores,db=db,tmp=tmp)
	os.system(cmd)

db_path = path + '/f05_gemini_db'
if not os.path.exists(db_path):
	os.mkdir(db_path)

db = db_path + '/' + family + '.db'
ped = path + '/filter_SZ.ped'
tmp = path + '/tmp'
load_vcf2gemini(anno_vcf, db, ped, cores, tmp)
