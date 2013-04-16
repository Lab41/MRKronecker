package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.LongSequenceInputFormat;

/**
 * This is a driver for a map-only job that generates a stochastic kronecker graph.
 *
 * The driver expects the following arguments:
 *      outputPath
 *      n - log_2(N) - where N is the number of nodes in the graph (the number of nodes in a graph generator using this
 *          method should always be a power of two.
 *      t_11, t_12, t_21, t_31 - the stochasic initator matrix. The sum of these variables should be 1.
 *
 *
 *
 * The arguments should be provided as follows :
 *
 *  StochasticKorneckerDriver outputPath n t_11 t_12 t_21 t_31
 *
 * @author kramachandran
 */
public class StochasticKorneckerDriver extends Configured implements Tool {
        private String outputPath;
        private
        private float[] initiator = new float[4];
        private int n;

        public void parseArgs(String[] args)
        {
        }

        @Override
        public int run(String[] args) throws Exception {
            parseArgs(args)
            /** configure Job **/
            Job job = new Job(getConf(), "DataIngest Example");
            job.setJarByClass(StochasticKorneckerDriver.class);


            LongSequenceInputFormat longSequenceInputFormat = new LongSequenceInputFormat()


            FileOutputFormat.setOutputPath(job, mrOutput);

            job.setMapperClass(StochasticKorneckerMapper.class);
            job.setReducerClass(EdgeListToFaunusAnnotatingReducer.class);



            if (job.waitForCompletion(true)) {
                return 0;
            } else {
                return 1;
            }
        }

        public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new StochasticKorneckerDriver(), args);

            System.exit(exitCode);
        }
    }
}
