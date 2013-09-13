package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class that operates on LongWritable-FaunusVertex pairs,
 * taking all nodes with the same IDs, combining their edges, and
 * generating a single FaunusVertex from them.
 * 
 * @author ndesai
 */
public class AnnotatingVertexReducer extends Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {
    private FaunusVertex faunusVertex = new FaunusVertex();
    private static final int NUM_PROPERTIES = 10;
    
    /**
     * Annotates the given FaunusVertex with a random UUID, random name,
     * ten random floats, and ten random strings.
     * @param vertex 
     */
    protected void annotate(FaunusVertex vertex) {
        vertex.setProperty("uuid", UUID.randomUUID().toString());
        vertex.setProperty("name", UUID.randomUUID().toString());
        //TODO: Change the number and size of variables to be configurable.
        //Perhaps based on some configuration or XML file?

        //Add a bunch of longs
        for (int i = 0; i < NUM_PROPERTIES; i++) {
            //TODO: change to the CERN random generator.. much faster
            vertex.setProperty("randLong" + Integer.toString(i), Math.random());
        }

        //Add a bunch of random strings
        for (int i = 0; i < NUM_PROPERTIES; i++) {
            vertex.setProperty("randString" + Integer.toString(i), RandomStringUtils.randomAlphanumeric((int) Math.floor(Math.random() * 150)));
        }
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
