import os
ped = '/hpc/grid/hgcb/workspace/users/shangzhong/SCZ/pedigree.ped'
fam_idv = {}
path = '/hpc/grid/hgcb/workspace/users/shangzhong/SCZ/f04_family_vcf'

with open(ped) as f:
    next(f)
    for line in f:
        item = line.strip().split('\t')
        fam = item[0]
        idv = item[1]
        fam_idv.setdefault(fam, []).append(idv)

db_path = '/hpc/grid/hgcb/workspace/users/shangzhong/SCZ/family_capture_error'
if not os.path.exists(db_path): os.mkdir(db_path)


import time
for k in fam_idv:
	out=path+'/'+k+'.vcf.gz'
	if os.path.exists(db_path+'/tmp/'+k+'.vcf.chunk0.db'):
		continue
	cmd = 'bsub -n 16 -M 11457280 -R \"span[ptile=16]\" -q medium -o {k}.log.txt \"gemini load -v {vcf} -p {ped} -t VEP --cores {cores} {db} --tempdir {tmp}\"'.format(vcf=out,ped=ped,cores=12,db=db_path+'/'+k+'.db',tmp=db_path+'/'+'tmp',k=k)
	# os.system(cmd)
	# os.system('rm {k}.log.txt'.format(k=k))
	print(cmd)
