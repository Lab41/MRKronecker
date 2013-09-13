/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.dendrite.generator.kronecker.mapreduce.BaseDriver;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.RangeInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class that runs a map-only job to create all 2^n vertices of a
 * stochastic Kronecker graph.
 * 
 * @author ndesai
 */
public class VertexCreationDriver extends BaseDriver implements Tool {
    Logger logger = LoggerFactory.getLogger(VertexCreationDriver.class);

    @Override
    public Job configureGeneratorJob(Configuration conf) throws IOException {
        Job job = new Job(getConf());
        job.setJobName("FastStochasticKroneckerVertexCreation N=" + Integer.toString(n));
        job.setJarByClass(VertexCreationDriver.class);

        job.setMapperClass(VertexCreationMapper.class);        
        job.setNumReduceTasks(0);

        /* Configure Input Format to be our custom InputFormat */
        job.setInputFormatClass(RangeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        /* Configure Map Output */
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FaunusVertex.class);

        //Set the configuration
        job.getConfiguration().set(Constants.PROBABILITY_MATRIX, initiator);
        job.getConfiguration().set(Constants.N, Integer.toString(n));
        job.getConfiguration().set(Constants.BLOCK_SIZE, Long.toString(1 << 20));
        return job;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new VertexCreationDriver(), args);

        System.exit(exitCode);
    }
}
