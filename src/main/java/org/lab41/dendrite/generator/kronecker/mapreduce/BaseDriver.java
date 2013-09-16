package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author kramachandran
 */
public abstract class BaseDriver extends Configured {
    Path outputPath;
    String initiator;
    int n;





    protected boolean parseArgs(String[] args) {

        if (args.length != 6) {
            return false;

        } else {
            //output path
            outputPath = new Path(args[0]);

            //read n
            n = Integer.parseInt(args[1]);
            if (n > 63) {
                return false;
            }

            //read the initator matrix
            String t_11 = args[2];
            String t_12 = args[3];
            String t_21 = args[4];
            String t_22 = args[5];

            initiator = t_11 + ", " + t_12 + ", " + t_21 + ", " + t_22;


        }
        return true;

    }

    public abstract Job configureGeneratorJob(Configuration conf) throws IOException;
}
