#!/bin/bash
[ -z BENCH_HOME ] && echo "Il faut sourcer \$BENCH_HOME/init.sh avant" && exit 1

WDIR=$BENCH_HOME/workload_01
log=$WDIR/compteurs.workload_01.$1.csv
## On ne peut pas utiliser ce time pour calculer une fonction ou un sous-shell :( !!!
TIME="/usr/bin/time -f real;%E;user;%U;sys;%S; -a -o $log "

pre_neo4j() {
    #$NEO4J_HOME/bin/neo4j-shell -c "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r ;" >/dev/null
    #$NEO4J_HOME/bin/neo4j-shell -c "DROP CONSTRAINT ON (c:Commune) ASSERT c.libcom IS UNIQUE;" >/dev/null
    #$NEO4J_HOME/bin/neo4j-shell -c "DROP INDEX ON :Voie (libelle);" >/dev/null
    #$NEO4J_HOME/bin/neo4j-shell -c "DROP CONSTRAINT ON (v:Voie) ASSERT v.sti IS UNIQUE;" >/dev/null
    #$NEO4J_HOME/bin/neo4j-shell -c "DROP INDEX ON :Numero (lib_off);" >/dev/null
    #$NEO4J_HOME/bin/neo4j-shell -c "DROP INDEX ON :Numero (no);" >/dev/null
    $NEO4J_HOME/bin/neo4j stop
    rm -fr $NEO4J_HOME/data/graph.db/
    $NEO4J_HOME/bin/neo4j start
}
etl_neo4j() {
    $NEO4J_HOME/bin/neo4j-shell -host localhost -port 1337 -file $WDIR/neo4J-loader_metropole-communes.cypher -v
    $NEO4J_HOME/bin/neo4j-shell -host localhost -port 1337 -file $WDIR/neo4J-loader_metropole-voies.cypher -v
    $NEO4J_HOME/bin/neo4j-shell -host localhost -port 1337 -file $WDIR/neo4J-loader_metropole-numeros.cypher -v
}

pre_orientdb() {
    $ORIENTDB_HOME/bin/console.sh "connect localhost $ORIENTDBUSER $ORIENTDBPASS ; 
    drop database remote:localhost/$ORIENTDBBASE $ORIENTDBUSER $ORIENTDBPASS ;"
    $ORIENTDB_HOME/bin/console.sh "connect localhost $ORIENTDBUSER $ORIENTDBPASS ; 
    create database remote:localhost/$ORIENTDBBASE $ORIENTDBUSER $ORIENTDBPASS plocal graph ;"
}
etl_orientdb() {
    $ORIENTDB_HOME/bin/oetl.sh $WDIR/orient-loader_metropole-communes.json
    $ORIENTDB_HOME/bin/oetl.sh $WDIR/orient-loader_metropole-voies.json
    $ORIENTDB_HOME/bin/oetl.sh $WDIR/orient-loader_metropole-numeros.json
}

usage() { echo "Usage: $0 neo4j|orientdb"; exit 1; }
####MAIN
[[ $# -ne 1 ]] && usage
case $1 in
    "orientdb") pre_orientdb; ts ; $TIME $0 "exec_orientdb"; ts ;;
    "neo4j") pre_neo4j ; ts ; $TIME $0 "exec_neo4j"; ts ;;
    "exec_orientdb") etl_orientdb;;
    "exec_neo4j") etl_neo4j ;;
    *) usage
esac
