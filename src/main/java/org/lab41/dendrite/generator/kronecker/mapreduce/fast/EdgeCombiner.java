package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A combiner class that operates on NodeTuple-NullWritable pairs; reduces
 * multiple instances of NodeTuples generated by the EdgeCreationMapper
 * and writes a single instance to context.
 * 
 * @author ndesai
 */
public class EdgeCombiner extends Reducer<NodeTuple, NullWritable, NodeTuple, NullWritable> {    
    @Override
    protected void reduce(NodeTuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}