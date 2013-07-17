#!/bin/sh
#set -e
#cd `dirname $0`

edge_dir=mrkronecker/fast-edge-output
vertex_dir=mrkronecker/fast-vertex-output
output_dir=mrkronecker/fast-graph-output

if [[ $# != 5 ]]; then
    echo "Usage: Driver.sh <number of iterations> <t_11> <t_12> <t_21> <t_22>"
    echo "       Number of iterations should be less than 64."
    exit 1
fi

./EdgeGenerator.sh $edge_dir $@
./VertexGenerator.sh $vertex_dir $@
./GraphGenerator.sh $edge_dir $vertex_dir $output_dir

faunus_dir=faunus-conv/faunus-input

hadoop fs -rm $faunus_dir
hadoop fs -cp $output_dir $faunus_dir

my_dir=$PWD
faunus_script="$my_dir/Transformation.groovy"

cat >>$faunus_script <<EOF
g = FaunusFactory.open('$FAUNUS_HOME/bin/faunus-to-graphson.properties')
g._.submit()

h = FaunusFactory.open('$FAUNUS_HOME/bin/faunus-to-edgelist.properties')
h._.submit()
EOF

cd $FAUNUS_HOME
./bin/gremlin.sh -e $faunus_script

cd $my_dir
