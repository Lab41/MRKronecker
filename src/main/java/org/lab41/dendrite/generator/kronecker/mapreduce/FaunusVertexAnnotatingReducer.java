package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.FaunusEdge;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Combines Faunus vertices which use the same id.
 * @author kramachandran
 */
public class FaunusVertexAnnotatingReducer extends Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex>{
    private  FaunusVertex faunusVertex = new FaunusVertex();
    Set<Long> otherVertices = new HashSet<Long>();
    protected void annotate(FaunusVertex vertex) {
        vertex.setProperty("uuid", UUID.randomUUID().toString());
        vertex.setProperty("name", UUID.randomUUID().toString());
        //TODO: Change the number and size of variables to be configurable.
        //Perhaps based on some configuration or XML file?

        //Add a bunch of longs
        for (int i = 0; i < 10; i++) {
            //TODO: change to the CERN random generator.. much faster
            vertex.setProperty("randLong" + Integer.toString(i), Math.random());
        }

        //Add a bunch of random strings
        for (int i = 0; i < 10; i++) {
            vertex.setProperty("randString" + Integer.toString(i), RandomStringUtils.randomAlphanumeric((int) Math.floor(Math.random() * 150)));
        }
        
        
    }
    @Override
    protected void reduce(LongWritable key, Iterable<FaunusVertex> values, Context context) throws IOException, InterruptedException {
        faunusVertex.reuse(key.get());

        for(FaunusVertex value: values)
        {
            otherVertices.clear();
            
            for(Edge e: value.getEdges(Direction.OUT))
            {
                long otherID = ((FaunusVertex) e.getVertex(Direction.IN)).getIdAsLong();
                if(!otherVertices.contains(otherID))
                {
                    faunusVertex.addEdge(Direction.OUT, e.getLabel(), otherID);
                    otherVertices.add(otherID);
                }
            }
        }

        annotate(faunusVertex);

        context.write(NullWritable.get(), faunusVertex);
        context.getCounter("Completed", "Vertices").increment(1L);
    }
}