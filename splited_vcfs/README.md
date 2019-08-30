This pipeline is to get potential causal rare variants in schizophrenia.
The previous vcf files were called and annotated against hg19 genome using illumina starling2. Here we use gemini to analyze the mutations. Since gemini only works with VEP or snpEff annotations, we'll reannotate the vcf files.
1. We run **p01_rm_anno_change_sample_name.py**
 to remove the illumina annotation information. Change the sample name to the format of familyID_individualID. For the header in vcf file '##FORMAT=<ID=AD,Number=.', change '.' to R to make sure it's compatible with vt when normalizing the vcf files.
2. Then we merge the vcf files using bcftools, normalize them using vt and annotate them using VEP and then load into gemini.  
    (a) if you want to study a single family, run **python p02_merge_norm_anno_vcf.py -f family_ID**. If family_ID is not provide, it considers all the families and merge all samples together.  
    (b) if you want to study all the families, run **python merge_families_separately.py**. This will run the command in (a) for each family available.  

Things to keep in mind using VEP:  
	* VEP version and the preindexed version of the database need to match.  
	* This python script parallelize vcf annoation by submitting each annoation command into a different core. We tried multiprocessing in python, but it doesn't work in hpc.  
