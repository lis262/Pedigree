This pipeline is to get poential causal rare variants in schizophrenia.
The previous vcf files were called and annotated against hg19 genome using illumina starling2. Here we use gemini to study the mutations. Since gemini only works with VEP or snpEff annotations, we'll reannotate the vcf files.
1. We run ** python p01_remove_vcf_info.py **
 to remove the illumina annotation information.
2. Then we merge the vcf file. run ** python p02_merge_vcf.py **
3. Normalize vcf file to make sure variants is left aligned and no left/right same nt between reference and alternative.
3. Then we run ** python p02_run_VEP.py ** to annotate the vcf files using VEP. Things to keep in mind using VEP:
	* VEP version and the preindexed version of the database need to match.
	* This python script parallelize vcf annoation by submitting each annoation command into a different core. We tried multiprocessing in python, but it doesn't work in hpc.
