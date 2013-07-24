package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.lab41.dendrite.generator.kronecker.mapreduce.StochasticKroneckerBaseMapper;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.RangeInputSplit;

/**
 * Mapper class that converts RangeInputSplit-NullWritable pairs (representing
 * a range of vertex IDs to be generated) into LongWritable-FaunusVertex
 * pairs (representing an ID and a vertex with that ID).
 * 
 * @author ndesai
 */
public class VertexCreationMapper extends StochasticKroneckerBaseMapper<RangeInputSplit, NullWritable, LongWritable, FaunusVertex>{
    private LongWritable nodeID = new LongWritable();
    protected long dimNodes = 0l;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.dimNodes = 1 << this.n;
    }
    
    @Override
    protected void map(RangeInputSplit key, NullWritable value, Context context) throws IOException, InterruptedException {
        for (long i = key.getStart(); i <= key.getEnd(); i++)
        {
            FaunusVertex node = createVertex(i);
            nodeID.set(i);
            context.write(nodeID, node);
        }
    }
}
