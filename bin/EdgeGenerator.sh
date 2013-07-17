#!/bin/sh
#set -e
#cd `dirname $0`

output=$1

msg1="Usage: EdgeGenerator.sh <edges output directory> <number of iterations> <t_11> <t_12> <t_21> <t_22>"
msg2="       Number of iterations should be less than 64."

if [[ $# != 6 ]]; then
    echo "$msg1"
    echo "$msg2"
    exit 1
fi

if [[ "$1" == "" ]]; then
    echo no output specified 1>&2
    exit 1
fi

hadoop fs -rm -r -f "$output*" || :

if [[ "$DEBUG_ENABLED" -eq 1 ]]; then
	HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
	shift
	hadoop jar ../target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.EdgeCreationDriver $@
else
	hadoop jar ../target/MRKronecker-1.0-SNAPSHOT.jar org.lab41.dendrite.generator.kronecker.mapreduce.fast.EdgeCreationDriver $@
fi
