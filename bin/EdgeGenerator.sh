#!/bin/sh

set -x -e

if [[ $# != 7 ]]; then
    echo "Usage: EdgeGenerator.sh <edges output directory> <number of annotations> <number of iterations> <t_11> <t_12> <t_21> <t_22>"
    echo "       Number of iterations should be less than 64, number of annotations less than 21."
    exit 1
fi

ROOT=$(cd $(dirname "$0")/..; pwd -P)
output_dir=$1

if [[ "$output_dir" == "" ]]; then
    echo no output specified 1>&2
    exit 1
fi

hadoop fs -rm -r -f "$output_dir*" || :

if [[ "$DEBUG_ENABLED" -eq 1 ]]; then
	HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
	shift
	hadoop jar $ROOT/target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.EdgeCreationDriver $@
else
	hadoop jar $ROOT/target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.EdgeCreationDriver $@
fi
