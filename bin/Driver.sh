#!/bin/sh

set -x -e

if [[ $# != 7 ]]; then
    echo "Usage: Driver.sh <project directory> <number of annotations> <number of iterations> <t_11> <t_12> <t_21> <t_22>"
    echo "       Number of iterations should be less than 64, number of annotations less than or equal to 20."
    exit 1
fi

if [[ "$FAUNUS_HOME" -eq "" ]]; then
    echo "set the FAUNUS_HOME environment variable to point at where faunus is installed"
    exit 1
fi

ROOT=$(cd $(dirname "$0")/..; pwd -P)

#Output paths in HDFS.
project_dir=$1
edge_dir=$project_dir/edges
vertex_dir=$project_dir/vertices
output_dir=$project_dir/graph
graphson_dir=$project_dir/graphson

num_annotations=$2

shift 1 #$@ is now [<number of annotations> <number of iterations> <t_11> <t_12> <t_21> <t_22>]

$ROOT/bin/EdgeGenerator.sh $edge_dir $@
$ROOT/bin/VertexGenerator.sh $vertex_dir $@
$ROOT/bin/GraphGenerator.sh $num_annotations $edge_dir $vertex_dir $output_dir
$ROOT/bin/GraphSONGenerator.sh $output_dir $graphson_dir
