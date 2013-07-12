package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.MatrixBlockInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is a driver for a map-only job that generates a stochastic Kronecker graph.
 * <p/>
 * The driver expects the following arguments:
 * <ul>
 * <li>outputPath</li>
 * <li>n - log_2(N) - where N is the number of nodes in the graph (the number of nodes in a graph generator using this</li>
 * method should always be a power of two.
 * <li>t_11, t_12, t_21, t_31 - the stochastic initiator matrix. The sum of these variables should be 1.</li>
 * </ul>
 * <p/>
 * <p/>
 * <p/>
 * The arguments should be provided as follows:
 * <p/>
 * StochasticKroneckerDriver outputPath n t_11 t_12 t_21 t_31
 * <p/>
 * This version of the driver uses the {@link MatrixBlockInputFormat} as the input format, and the
 * {@link FileOutputFormat} as the output format.
 *
 * @author kramachandran
 */
public class StochasticKroneckerDriver extends BaseDriver implements Tool {
    Logger logger = LoggerFactory.getLogger(StochasticKroneckerDriver.class);

    @Override
    public Job configureGeneratorJob(Configuration conf) throws IOException {
        /** configure Job **/
        Job job = new Job(getConf());
        job.setJobName("StochasticKronecker N = " + Integer.toString(n));
        job.setJarByClass(StochasticKroneckerDriver.class);

        /** Set the Mapper & Reducer**/
        job.setMapperClass(StochasticKroneckerFaunusVertexGeneratorMapper.class);
        job.setReducerClass(FaunusVertexAnnotatingReducer.class);


            /* Configure Input Format to be our custom InputFormat */
        job.setInputFormatClass(MatrixBlockInputFormat.class);
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
        job.getConfiguration().set(Constants.BLOCK_SIZE, Long.toString((long) Math.pow(2, 16)));
        return job;
    }

    @Override
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
        int exitCode = ToolRunner.run(new StochasticKroneckerDriver(), args);

        System.exit(exitCode);
    }
}

