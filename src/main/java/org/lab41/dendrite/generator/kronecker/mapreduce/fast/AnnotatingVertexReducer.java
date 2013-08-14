package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.lab41.dendrite.generator.kronecker.mapreduce.AnnotatingBaseReducer;

/**
 * Reducer class that operates on LongWritable-FaunusVertex pairs,
 * taking all nodes with the same IDs, combining their edges, and
 * generating a single FaunusVertex from them.
 * 
 * @author ndesai
 */
public class AnnotatingVertexReducer extends AnnotatingBaseReducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {
    private FaunusVertex faunusVertex = new FaunusVertex();
    
    protected void annotate(FaunusVertex element) {
        element.setProperty("uuid", UUID.randomUUID().toString());
        element.setProperty("name", UUID.randomUUID().toString());
        super.annotate(element);
    }
    
    /**
     * Reduces LongWritable-FaunusVertex pairs by combining the edges of all
     * nodes with the same ID. Uses the addAll method of a FaunusVertex
     * to add edges and thus does not check for duplicates.
     * 
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    protected void reduce(LongWritable key, Iterable<FaunusVertex> values, Context context) throws IOException, InterruptedException {
        faunusVertex.reuse(key.get());
        for(FaunusVertex value : values) {
            faunusVertex.addAll(value);
        }
        
        annotate(faunusVertex);
        context.write(NullWritable.get(), faunusVertex);
    }
}
