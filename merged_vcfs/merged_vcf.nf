#!/usr/bin/env nextflow

/* ----------- define parameters ---------------------
*/
// (1) related files
ref_fa   =  file(params.genome_fa)
ref_fai  =  ref_fa + '.fai'
ref_dict =  ref_fa.parent / ref_fa.baseName + '.dict'
if (!ref_fa.exists()) exit 1, "Reference genome not found: ${ref_fa}"

cross_fa = file(params.crossmap_fa)
cross_fai = cross_fa + '.fai'
if (!cross_fa.exists()) exit 1, "New Reference genome not found: ${cross_fa}"


chain = file(params.ucsc_chain)
if (!chain.exists()) exit 1, "Genome cross chain file not found: ${chain}"

cohort_vcf = file(params.vcf_fn)
if (!cohort_vcf.exists()) exit 1, "cohort vcf file not found: ${cohort_vcf}"

vep_db = file(params.vep_db)
if (!vep_db.exists()) exit 1, "pedigree file not found: ${vep_db}"

ped = file(params.ped_fn)
if (!ped.exists()) exit 1, "pedigree file not found: ${ped}"

// (2) check environment resource
ava_mem = (double) (Runtime.getRuntime().freeMemory())
ava_cpu = Runtime.getRuntime().availableProcessors()

if (params.cpu != null && ava_cpu > params.cpu) {
    ava_cpu = params.cpu
}

if (params.mem != null && ava_mem > params.mem) {
    ava_mem = params.mem
}

/*------------ normalize vcf file ---------------------
*/
process Normalize {
	label 'one_core'

	publishDir pattern: "*",
        path: {params.out_dir + '/f01_normalize'},
        mode: 'copy', overwrite: true

	input:
	file ref_fa
	file cohort_vcf

	output:
	file "${cohort_vcf.simpleName}.norm.vcf.gz" into norm_vcf
	file "${cohort_vcf.simpleName}.norm.vcf.gz.tbi" into norm_vcf_idx
	script:
	"""
	vt decompose -s ${cohort_vcf} | vt normalize -r ${ref_fa} -o ${cohort_vcf.simpleName}.norm.vcf.gz -
	tabix ${cohort_vcf.simpleName}.norm.vcf.gz
	echo ${cohort_vcf.simpleName}.norm.vcf.gz.tbi
	"""
}


/*----------- annotate vcf file ------------------------
*/
process Annotate {
	label 'four_core'
	tag "vep_annotate"
	publishDir pattern: "*",
        path: {params.out_dir + '/f02_annotate'},
        mode: 'copy', overwrite: true

	input:
	file norm from norm_vcf
	file norm_idx from norm_vcf_idx
	file vep_db

	output:
	file "${norm.simpleName}.vep.vcf.gz" into vep_vcf
	file "${norm.simpleName}.vep.vcf.gz.tbi" into vep_vcf_idx

	script:
	"""
	vep -i ${norm} -o ${norm.simpleName}.vep.vcf.gz --vcf \
	--compress_output bgzip --cache --dir ${vep_db} \
	--sift b --polyphen b --symbol --numbers --biotype \
	--total_length --fields Consequence,Codons,Amino_acids,Gene,SYMBOL,Feature,EXON,PolyPhen,SIFT,Protein_position,BIOTYPE \
	--offline --fork 4 --force_overwrite
	tabix ${norm.simpleName}.vep.vcf.gz
	echo ${norm.simpleName}.vep.vcf.gz.tbi
	"""
}


/*---------- transfer vcf position between assemblies ---
*/
process CrossMap {
	label 'one_core'
	publishDir pattern: "*.vcf*",
        path: {params.out_dir + '/f03_crossmap'},
        mode: 'copy', overwrite: true
	
	input:
	file cross_fa
	file cross_fai
	file chain
	file anno_vcf from vep_vcf
	file anno_vcf_idx from vep_vcf_idx

	output:
	file "${anno_vcf.simpleName}.crossmap.sort.vcf.gz" into crossmap_vcf
	file "${anno_vcf.simpleName}.crossmap.sort.vcf.gz.tbi" into crossmap_vcf_idx
	file "temp" into crossmap_waiting

	script:
	"""
	CrossMap.py vcf ${chain} ${anno_vcf} ${cross_fa} ${anno_vcf.simpleName}.crossmap.vcf
	bgzip ${anno_vcf.simpleName}.crossmap.vcf
	mkdir tmp
	bcftools sort -O z -o ${anno_vcf.simpleName}.crossmap.sort.vcf.gz -T tmp ${anno_vcf.simpleName}.crossmap.vcf.gz
	tabix ${anno_vcf.simpleName}.crossmap.sort.vcf.gz
	echo ${anno_vcf.simpleName}.crossmap.sort.vcf.gz.tbi
	echo "finish crossmap" > temp
	"""
}


/*---------- split vcf into families ------------------
*/

process Fam_Idv_pair {
	
    input:
    file ped
    file temp from crossmap_waiting.toList()

    output:
    stdout fam_idv_dict

    script:
    """
    #!/usr/bin/env python
    fam_idv = {}
    with open('${ped}') as f:
        next(f)
        for line in f:
            item = line.strip().split('\t')
            fam = item[0]
            idv = item[1]
            fam_idv.setdefault(fam, []).append(idv)
    for k in fam_idv:
    	print(k+' '+','.join(fam_idv[k]))        
	"""
}


fam_idv_dict
    .splitText()
    .map {it.split()}
    .spread(crossmap_vcf)
    .set {crossmap_vcf_for_sep}


process ExtractFamily {
	label 'one_core'
	publishDir pattern: "*",
        path: {params.out_dir + '/f04_family_vcf'},
        mode: 'copy', overwrite: true
    
    input:
    set val(fam), val(idv), file(vcf) from crossmap_vcf_for_sep

    output:
    set val(fam), file("${fam}.vcf.gz"), file("${fam}.vcf.gz.tbi") into fam_vcf

    script:
    """   
    bcftools view -Oz -s ${idv} ${vcf} -o ${fam}.vcf.gz
    tabix ${fam}.vcf.gz
    echo ${fam}.vcf.gz.tbi
    """
}


/*---------- Load into GEMINI ------------------
*/
process LoadToGemini {
	label 'more_cores'

	publishDir pattern: "*",
	        path: {params.out_dir + '/f05_family_db'},
	        mode: 'copy', overwrite: true
	
	input:
	set val(fam), file(vcf), file(vcf_idx) from fam_vcf
	file ped

	output:


	script:
	"""
	mkdir tmp
	gemini load -v ${vcf} -p ${ped} -t VEP --cores ${ava_cpu} ${fam}.db --tempdir tmp
	"""
}
