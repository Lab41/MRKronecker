#!/bin/sh
set -e
cd `dirname $0`

output=$1

if [[ "$1" == "" ]]; then
    echo no output specified 1>&2
    exit 1
fi


hadoop fs -rm -r -f "$3*" || :

if [[ "$DEBUG_ENABLED" -eq 1 ]]; then
	HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
	shift
	hadoop jar ../target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.StochasticKroneckerDriver $@
else
	hadoop jar ../target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.StochasticKroneckerDriver $@
fi