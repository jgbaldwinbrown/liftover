#!/bin/bash
set -e

OLDFAZ=$1
NEWFAZ=$2
OUT=$3

cat "${OLDFAZ}" | gunzip -c > old.fa
cat "${NEWFAZ}" | gunzip -c > new.fa

cat old.fa | splitfa -o old_
cat new.fa | splitfa -o new_

ls old_*.fa | while read OLD ; do
	CHR=`echo "$OLD" | sed 's/old_//' | sed 's/\.fa//'`
	NEW="new_${CHR}.fa"
	if [ -s "$NEW" ] ; then
		lastz "$OLD" "$NEW" > "${OUT}_${CHR}_aln.lav"
		lavToAxt -fa -tfa "${OUT}_${CHR}_aln.lav" "$OLD" "$NEW" "${OUT}_${CHR}_aln.axt"
		axtChain -faT -faQ -linearGap=medium "${OUT}_${CHR}_aln.axt" old.fa new.fa "${OUT}_${CHR}_aln.chain"
	fi
done

chainMergeSort "${OUT}_*_aln.chain" | chainSplit chain stdin

fachrlens old.fa > old.chrom.sizes
fachrlens new.fa > new.chrom.sizes

cd chain
mkdir -p ../net

ls *.chain | while read i ; do
	a=`basename "$i" .chain`
	chainNet "${a}.chain" ../old.chrom.sizes ../new.chrom.sizes "../net/${a}.net" /dev/null
	mkdir -p ../over
	netChainSubset "../net/${a}.net" "${a}.chain" "../over/${a}.chain"
done
