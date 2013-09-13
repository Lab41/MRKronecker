package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.dendrite.generator.kronecker.mapreduce.BaseDriver;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.QuotaInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class for the generation of edges of a stochastic Kronecker graph
 * via the Kron-Gen algorithm.
 * 
 * @author ndesai
 */
public class EdgeCreationDriver extends BaseDriver implements Tool {
    Logger logger = LoggerFactory.getLogger(EdgeCreationDriver.class);

    @Override
    public Job configureGeneratorJob(Configuration conf) throws IOException {
        Job job = new Job(getConf());
        job.setJobName("FastStochasticKroneckerEdgeCreation N=" + Integer.toString(n));
        job.setJarByClass(EdgeCreationDriver.class);

        /** Set the Mapper & Reducer**/
        job.setMapperClass(EdgeCreationMapper.class);
        job.setCombinerClass(EdgeCombiner.class);
        job.setReducerClass(EdgeReducer.class);

        /* Configure Input Format to be our custom InputFormat */
        job.setInputFormatClass(QuotaInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        /* Configure Map Output */
        job.setMapOutputKeyClass(NodeTuple.class);
        job.setMapOutputValueClass(NullWritable.class);

        /* Configure job (Reducer) output */
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FaunusVertex.class);

        //Set the configuration
        job.getConfiguration().set(Constants.PROBABILITY_MATRIX, initiator);
        job.getConfiguration().set(Constants.N, Integer.toString(n));
        job.getConfiguration().set(Constants.BLOCK_SIZE, Long.toString(1 << 20));
        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EdgeCreationDriver(), args);

        System.exit(exitCode);
    }
}
