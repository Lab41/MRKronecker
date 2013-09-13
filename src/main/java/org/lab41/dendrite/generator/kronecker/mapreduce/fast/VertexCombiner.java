package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner class that reduces LongWritable-FaunusVertex pairs (representing
 * vertices with the same node ID in a stochastic Kronecker graph) by adding
 * all their edges to a single FaunusVertex (representing a vertex with
 * the same node ID).
 * 
 * @author ndesai
 */
public class VertexCombiner extends Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex> {
    private FaunusVertex faunusVertex = new FaunusVertex();
    
    @Override
    protected void reduce(LongWritable key, Iterable<FaunusVertex> values, Context context) throws IOException, InterruptedException {
        faunusVertex.reuse(key.get());
        for(FaunusVertex value : values) {
            faunusVertex.addAll(value);
        }
        
        context.write(key, faunusVertex);
    }
}
