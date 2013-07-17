/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.dendrite.generator.kronecker.mapreduce.BaseDriver;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.lab41.dendrite.generator.kronecker.mapreduce.FastStochasticKroneckerDriver;

/**
 *
 * @author ndesai
 */
public class FastStochasticKroneckerGraphCreationDriver extends BaseDriver implements Tool {

    @Override
    public Job configureGeneratorJob(Configuration conf) throws IOException {
        Job job = new Job(getConf());
        job.setJobName("FastStochasticKroneckerGraphCreation N=" + Integer.toString(n));
        job.setJarByClass(FastStochasticKroneckerGraphCreationDriver.class);

        /** Set the Mapper & Reducer**/
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(FastStochasticKroneckerVertexCombiner.class);
        job.setReducerClass(FastStochasticKroneckerAnnotatingVertexReducer.class);

        /* Configure Input Format to be our custom InputFormat */
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        /* Configure Map Output */
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);

        /* Configure job (Reducer) output */
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);

        //Set the configuration
        job.getConfiguration().set(Constants.PROBABILITY_MATRIX, initiator);
        job.getConfiguration().set(Constants.N, Integer.toString(n));
        job.getConfiguration().set(Constants.BLOCK_SIZE, Long.toString(1 << 20));
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
            System.out.println("Usage : outputPath n t_11 t_12 t_21 t_31 \n" +
                    "               n must be less than 64");
            return 1;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FastStochasticKroneckerDriver(), args);

        System.exit(exitCode);
    }
    
}
