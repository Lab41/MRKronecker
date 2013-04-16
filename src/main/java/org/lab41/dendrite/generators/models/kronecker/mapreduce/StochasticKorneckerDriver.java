package org.lab41.dendrite.generators.models.kronecker.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

/**
 * This is a driver for a map-only job that generates a stochastic kronecker graph.
 *
 * The driver expects the following arguments:
 *      n - log_2(N) - where N is the number of nodes in the graph (the number of nodes in a graph generator using this
 *          method should always be a power of two.
 *      t_11, t_12, t_21, t_31 - the stochasic initator matrix.
 *
 * The arguments should be provided as follows :
 *
 *  StochasticKorneckerDriver 10 0.1 0.2 0.4 0.3
 *
 * @author kramachandran
 */
public class StochasticKorneckerDriver Configured implements Tool {


        @Override
        public int run(String[] args) throws Exception {
            Path mrInput, mrOutput;
            if (args.length == 2) {
                mrInput = new Path(args[0]);
                mrOutput = new Path(args[1] + directoryFormat.format(new Date()));
            } else {
                System.err.println("Parameter missing!");
                return 1;
            }

            /** configure Job **/
            Job job = new Job(getConf(), "DataIngest Example");
            job.setJarByClass(Driver.class);
            job.setUserClassesTakesPrecedence(true);

            FileInputFormat.setInputPaths(job, mrInput);
            FileOutputFormat.setOutputPath(job, mrOutput);

            job.setMapperClass(MapperRawToAvro.class);
            job.setReducerClass(ReducerByDateTime.class);

            AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
            AvroJob.setMapOutputValueSchema(job, SampleRecord.SCHEMA$);

            AvroKeyOutputFormat.setCompressOutput(job, true);
            AvroKeyOutputFormat.setOutputCompressorClass(job, DeflateCodec.class);

            AvroMultipleOutputs.addNamedOutput(job, "sampleRecord",
                    AvroKeyOutputFormat.class, SampleRecord.SCHEMA$);
            MultipleOutputs.setCountersEnabled(job, true);

            if (job.waitForCompletion(true)) {
                return 0;
            } else {
                return 1;
            }
        }

        public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new Driver(), args);

            System.exit(exitCode);
        }
    }
}
