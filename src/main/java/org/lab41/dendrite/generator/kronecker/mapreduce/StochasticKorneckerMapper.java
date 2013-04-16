package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *
 * @author kramachandran
 */
public class StochasticKorneckerMapper extends Mapper<LongWritable, NullWritable, FaunusVertex, NullWritable> {
    @Override
    protected void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
