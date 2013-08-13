package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import java.io.IOException;
import org.apache.commons.lang.RandomStringUtils;
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
public class AnnotatingEdgeReducer extends Reducer<NodeTuple, NullWritable, LongWritable, FaunusVertex> {
    private LongWritable nodeID = new LongWritable();
    private FaunusVertex faunusVertex = new FaunusVertex();
    private static final int NUM_PROPERTIES = 10;

 
    protected void annotate(Edge edge) {
        for (int i = 0; i < NUM_PROPERTIES; i++) {
            //TODO: change to the CERN random generator.. much faster
            edge.setProperty("randLong" + Integer.toString(i), Math.random());
        }

        //Add a bunch of random strings
        for (int i = 0; i < NUM_PROPERTIES; i++) {
            edge.setProperty("randString" + Integer.toString(i), RandomStringUtils.randomAlphanumeric((int) Math.floor(Math.random() * 150)));
        }
    }
    
    @Override
    protected void reduce(NodeTuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        nodeID.set(key.getTail());
        faunusVertex.reuse(key.getTail());
        Edge addedEdge = faunusVertex.addEdge(Direction.OUT, "RELATIONSHIP", key.getHead());
        annotate(addedEdge);
        context.write(nodeID, faunusVertex);
    }
}
