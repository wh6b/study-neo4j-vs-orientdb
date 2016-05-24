#!/bin/bash
[ -z BENCH_HOME ] && echo "Il faut sourcer \$BENCH_HOME/init.sh avant" && exit 1

## Generations des fichiers sources pour l'urbanisation et les personnes
cat $BENCH_SOURCES/dicollecte_fr-toutesvariantes.dic  | awk -F '/|\t' '{print $1}' | egrep -v '^.{1,4}$' > $BENCH_DATA/nomsdefamille.csv
cat $BENCH_SOURCES/Voies.csv | perl -p -e 's/\r\n/\n/g' | sed 's/^\xEF\xBB\xBF//' | awk -F";" '{print $1";"$2";"$6";"$7";"$8";"$9";"}' > $BENCH_DATA/Voies.clean.csv
cat $BENCH_SOURCES/Numeros.csv | perl -p -e 's/\r\n/\n/g' | sed 's/^\xEF\xBB\xBF//' | awk -F";" '{sub(/^ /, "", $1);print $1";"$3";"$5";"$6";"$7";"$10";"$11";true;"}' > $BENCH_DATA/Numeros.clean.csv

## sed pour enlever les charactere BOM   en vi c est :set nobomb
## sinon $ vim +"argdo se nobomb | w" $BENCH_DATA/Numeros.clean.csv 
