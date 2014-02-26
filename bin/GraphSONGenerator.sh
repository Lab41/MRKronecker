#!/bin/sh

set -x -e

if [[ $# != 2 ]]; then
    echo "Usage: GraphSONGenerator.sh <graph SequenceFile input directory> <graph GraphSON output directory>"
    exit 1
fi

if [[ "$FAUNUS_HOME" -eq "" ]]; then
    echo "set the FAUNUS_HOME environment variable to point at where faunus is installed"
    exit 1
fi

ROOT=$(cd $(dirname "$0")/..; pwd -P)
input_dir=$1
output_dir=$2

#Write a script file to output.
faunus_script_file=$ROOT/Transformation.groovy
rm $faunus_script_file || true

cat >>$faunus_script_file <<EOF
g = new FaunusGraph()
g.setGraphInputFormat(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
g.setGraphOutputFormat(com.thinkaurelius.faunus.formats.graphson.GraphSONOutputFormat.class);
g.setSideEffectOutputFormat(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
g.setOutputLocationOverwrite(true);
g.setInputLocation('$input_dir')
g.setOutputLocation('$output_dir')
g._.submit()
EOF

pushd $FAUNUS_HOME
./bin/gremlin.sh -e $faunus_script_file
popd
rm $faunus_script_file
