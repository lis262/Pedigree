vcf=$1
out=$2
db=$3
vep -i $vcf -o $out --vcf --compress_output bgzip --cache -dir $db --sift b --polyphen b --symbol --numbers --biotype --total_length --fields Consequence,Codons,Amino_acids,Gene,SYMBOL,Feature,EXON,PolyPhen,SIFT,Protein_position,BIOTYPE --offline
tabix $out