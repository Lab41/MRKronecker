package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for generating a stochastic Kronecker graph from the outputs
 * of {@link EdgeCreationDriver} and {@link VertexCreationDriver}.
 * It takes as input a SequenceFile listing single edges
 * and SequenceFile listing vertices and combines them to form
 * a representation of a
 * single stochastic Kronecker graph as a collection of FaunusVertexes
 * with multiple edges coming off them.
 * 
 * @author ndesai
 */
public class GraphCreationDriver extends Configured implements Tool {
    protected Path edgeInputPath;
    protected Path vertexInputPath;
    protected Path outputPath;
    
    private static final String USAGE_STRING = "Usage: FastStochasticKroneckerGraphCreationDriver <edgeInputFilePath> <vertexInputFilePath> <outputPath>";
    private static final int NUM_ARGS = 3;
    
    protected boolean parseArgs(String[] args) {
        if (args.length != NUM_ARGS) return false;
                
        edgeInputPath = new Path(args[0]);
        vertexInputPath = new Path(args[1]);
        outputPath = new Path(args[2]);
        return true;
    }
    
    public Job configureGeneratorJob(Configuration conf) throws IOException {
        Job job = new Job(getConf());
        job.setJobName("FastStochasticKroneckerGraphCreation Edge="+edgeInputPath.toString() + 
                       " Vertex="+vertexInputPath.toString());
        job.setJarByClass(GraphCreationDriver.class);

        /** Set the Mapper & Reducer**/
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(VertexCombiner.class);
        job.setReducerClass(AnnotatingVertexReducer.class);

        /* Configure Input Format to be our custom InputFormat */
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleInputs.addInputPath(job, edgeInputPath, SequenceFileInputFormat.class);
        MultipleInputs.addInputPath(job, vertexInputPath, SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        /* Configure Map Output */
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);

        /* Configure job (Reducer) output */
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
        
        return job;
    }
    
    public int run(String[] args) throws Exception {
        if (parseArgs(args)) {
            Configuration conf = new Configuration();
            Job job = configureGeneratorJob(conf);

            if (job.waitForCompletion(true)) {
                return 0;
            } else {
                return 1;
            }
        } else {
            System.out.println(USAGE_STRING);
            return 1;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GraphCreationDriver(), args);

        System.exit(exitCode);
    }
    
}
