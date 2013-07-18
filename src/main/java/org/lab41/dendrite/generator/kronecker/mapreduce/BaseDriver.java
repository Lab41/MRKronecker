package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author kramachandran
 */
public abstract class BaseDriver extends Configured {
    protected Path outputPath;
    protected String initiator;
    protected int n;
    
    private static final String USAGE_STRING = 
               "Arguments: <outputPath> <number of iterations> t_11 t_12 t_21 t_22\n" +
               "           Number of iterations must be less than 64.";
    private static final int NUM_ARGS = 6;
    private static final int MAX_ITERATIONS = 63;

    protected boolean parseArgs(String[] args) {
        if (args.length != NUM_ARGS) {
            return false;
        } else {
            outputPath = new Path(args[0]);

            n = Integer.parseInt(args[1]);
            if (n > MAX_ITERATIONS) {
                return false;
            }

            String t_11 = args[2];
            String t_12 = args[3];
            String t_21 = args[4];
            String t_22 = args[5];
            initiator = t_11 + ", " + t_12 + ", " + t_21 + ", " + t_22;
        }
        return true;
    }
    
    public abstract Job configureGeneratorJob(Configuration conf) throws IOException;

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
}
