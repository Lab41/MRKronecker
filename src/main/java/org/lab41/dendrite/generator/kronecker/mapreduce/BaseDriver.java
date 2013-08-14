package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Base class for any classes that drive the generation of elements of a
 * stochastic Kronecker graph from a 2x2 initiator matrix.
 * Subclasses of this class will take six command-line
 * arguments - an output path, the number of iterations (must be less
 * than 64), and the four elements of the initiator matrix.
 * From this it will generate a stochastic Kronecker graph with
 * 2^n nodes, where n is the number of iterations specified.
 * 
 * @author kramachandran
 */
public abstract class BaseDriver extends Configured {
    protected int numAnnotations;
    protected Path outputPath;
    protected String initiator;
    protected int n;
    
    private static final String USAGE_STRING = 
               "Arguments: <outputPath> <number of annotations> <number of iterations> t_11 t_12 t_21 t_22\n" +
               "           Number of iterations must be less than 64.";
    private static final int NUM_ARGS = 7;
    private static final int MAX_ITERATIONS = 63;
    private static final int MAX_ANNOTATIONS = 20;

    protected boolean parseArgs(String[] args) {
        if (args.length != NUM_ARGS) {
            return false;
        } else {
            outputPath = new Path(args[0]);

            numAnnotations = Integer.parseInt(args[1]);
            if(numAnnotations > MAX_ANNOTATIONS) return false;
            
            n = Integer.parseInt(args[2]);
            if(n > MAX_ITERATIONS) return false;

            String t_11 = args[3];
            String t_12 = args[4];
            String t_21 = args[5];
            String t_22 = args[6];
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
