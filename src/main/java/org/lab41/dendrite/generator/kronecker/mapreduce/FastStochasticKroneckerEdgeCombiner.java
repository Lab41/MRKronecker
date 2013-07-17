/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ndesai
 */
public class FastStochasticKroneckerEdgeCombiner extends Reducer<NodeTuple, NullWritable, NodeTuple, NullWritable> {
    private FaunusVertex faunusVertex = new FaunusVertex();
    
    @Override
    protected void reduce(NodeTuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //faunusVertex.reuse(key.getTail());
        //faunusVertex.addEdge(Direction.OUT, "RELATIONSHIP", key.getHead());        
        context.write(key, NullWritable.get());
    }
}
