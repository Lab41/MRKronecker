#!/bin/sh
#set -e
#cd `dirname $0`

msg="Usage: GraphGenerator.sh <number of annotations> <edge directory> <vertex directory> <graph output directory>"

if [[ $# != 4 ]]; then
    echo "$msg"
    exit 1
fi

output_dir=$4
hadoop fs -rm -r -f "$output_dir*" || :

if [[ "$DEBUG_ENABLED" -eq 1 ]]; then
	HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
	shift
	hadoop jar ../target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.GraphCreationDriver $@
else
	hadoop jar ../target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.GraphCreationDriver $@
fi
