/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 */
public class FastStochasticKroneckerEdgeReducer extends Reducer<NodeTuple, NullWritable, LongWritable, FaunusVertex> {
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
