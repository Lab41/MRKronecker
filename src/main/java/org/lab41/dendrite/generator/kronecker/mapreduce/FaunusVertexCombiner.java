package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Resolves conflicts when two map processes generate the same edge.
 * 
 * @author kramachandran
 */
public class FaunusVertexCombiner extends Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex> {
    private  FaunusVertex faunusVertex = new FaunusVertex();

    @Override
    protected void reduce(LongWritable key, Iterable<FaunusVertex> values, Context context) throws IOException, InterruptedException {

        faunusVertex.reuse(key.get());

        for(FaunusVertex value: values)
        {
            if(context.getConfiguration().getBoolean(Constants.CHECK_FOR_CONFLICTS, true))
            {
                //TODO: Add code to check for conflicts.
                
            }
            faunusVertex.addAll(value);
        }


        context.write(key, faunusVertex);
        context.getCounter("Combined", "Edges").increment(1L);
    }
}
