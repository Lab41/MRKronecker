#!/bin/sh
set -x
#cd `dirname $0`

if [[ $# != 7 ]]; then
    echo "Usage: Driver.sh <project directory> <number of annotations> <number of iterations> <t_11> <t_12> <t_21> <t_22>"
    echo "       Number of iterations should be less than 64, number of annotations less than or equal to 20."
    exit 1
fi

if [[ `dirname $0` != "." ]]; then
    echo "CD to my directory."
    exit 1
fi

my_dir=$PWD

#Output paths in HDFS.
project_dir=$1
edge_dir=$project_dir/edges
vertex_dir=$project_dir/vertices
output_dir=$project_dir/graph
graphson_dir=$project_dir/graphson

num_annotations=$2

shift 1 #$@ is now [<number of annotations> <number of iterations> <t_11> <t_12> <t_21> <t_22>]

./EdgeGenerator.sh $edge_dir $@
./VertexGenerator.sh $vertex_dir $@
./GraphGenerator.sh $num_annotations $edge_dir $vertex_dir $output_dir
./GraphSONGenerator.sh $output_dir $graphson_dir
