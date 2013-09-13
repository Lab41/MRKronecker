package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class that operates on NodeTuple-NullWritable pairs (representing
 * instances of an edge in a stochastic Kronecker graph), generating
 * a LongWritable-FaunusVertex pair (represents that edge's tail, with
 * the edge coming out of it).
 * 
 * @author ndesai
 */
public class EdgeReducer extends Reducer<NodeTuple, NullWritable, LongWritable, FaunusVertex> {
    private LongWritable nodeID = new LongWritable();
    private FaunusVertex faunusVertex = new FaunusVertex();
 
    @Override
    protected void reduce(NodeTuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        nodeID.set(key.getTail());
        faunusVertex.reuse(key.getTail());
        faunusVertex.addEdge(Direction.OUT, "RELATIONSHIP", key.getHead());
        context.write(nodeID, faunusVertex);
    }
}
