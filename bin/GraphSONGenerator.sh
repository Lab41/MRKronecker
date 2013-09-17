#!/bin/sh
set -x
#cd `dirname $0`

if [[ $# != 2 ]]; then
    echo "Usage: GraphSONGenerator.sh <graph SequenceFile input directory> <graph GraphSON output directory>"
    exit 1
fi

if [[ `dirname $0` != "." ]]; then
    echo "cd to `dirname $0` to run this script."
    exit 1
fi

input_dir=$1
output_dir=$2

#Write a script file to output.
faunus_script_file="Transformation.groovy"
rm $faunus_script_file 

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

faunus_dir=$FAUNUS_HOME
cd $faunus_dir
./bin/gremlin.sh -e $faunus_script_file

cd $my_dir
rm $faunus_script_file
