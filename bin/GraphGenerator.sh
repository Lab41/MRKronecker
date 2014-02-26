#!/bin/sh

set -x -e

msg="Usage: GraphGenerator.sh <number of annotations> <edge directory> <vertex directory> <graph output directory>"

if [[ $# != 4 ]]; then
    echo "$msg"
    exit 1
fi

ROOT=$(cd $(dirname "$0")/..; pwd)
output_dir=$4
hadoop fs -rm -r -f "$output_dir*" || :

if [[ "$DEBUG_ENABLED" -eq 1 ]]; then
	HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
	shift
	hadoop jar $ROOT/target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.GraphCreationDriver $@
else
	hadoop jar $ROOT/target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.GraphCreationDriver $@
fi
