#!/bin/sh
#set -e
#cd `dirname $0`

edge_dir=$1
vertex_dir=$2
output_dir=$3
others="${@:4}"

edge_file=$edge_dir/part-r-00000
vertex_file=$vertex_dir/part-m-00000

msg1="Usage: KroneckerGenerator.sh <edge intermediate directory> <vertex intermediate directory> <graph output_dir directory> <number of iterations> <t_11> <t_12> <t_21> <t_22>"
msg2="       Number of iterations should be less than 64."

if [[ $# != 8 ]]; then
    echo "$msg1"
    echo "$msg2"
    exit 1
fi

./EdgeGenerator.sh $edge_dir $others
./VertexGenerator.sh $vertex_dir $others
./GraphGenerator.sh $edge_file $vertex_file $output_dir
