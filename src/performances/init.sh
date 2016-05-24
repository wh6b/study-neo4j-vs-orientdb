set -x
#export BENCH_HOME=/home/user42/Work/ENG221
export BENCH_HOME=$(cd `dirname $0` && pwd)

export BENCH_DATA=$BENCH_HOME/data/
export BENCH_SOURCES=$BENCH_HOME/sources/
export PYTHONPATH=.:$BENCH_HOME:$PYTHONPATH
#export PYTHON_PATH=.:$BENCH_HOME:$PYTHON_PATH
export ORIENTDB_HOME=~/SGBD/orientdb-community-2.0.2
export NEO4J_HOME=~/SGBD/neo4j-community-2.1.7

export NEO4J_DATABASE=$BENCH_HOME/databases/neo4j.metropole.db

export NEO4JDBSTR="http://tk3:7474/db/data/"
#export NEO4JDBSTR="http://192.168.1.1:7474/db/data/"


export ORIENTDBSERV="tk3"
#export ORIENTDBSERV="192.168.1.1"
#export ORIENTDBSERV="localhost"
export ORIENTDBPORT=2424

export ORIENTDBBASE="etl1"
export ORIENTDBUSER="root"
export ORIENTDBPASS="root"

export PATH=$ORIENTDB_HOME/bin:$NEO4J_HOME/bin:$PATH

function ts() { echo "`date '+%Y%m%d %H:%M%S'`"; }
export -f ts
set +x

echo "Environnement initialisé."
