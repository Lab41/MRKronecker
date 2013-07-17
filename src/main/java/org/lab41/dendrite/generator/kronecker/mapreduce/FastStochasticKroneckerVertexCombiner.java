/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ndesai
 */
public class FastStochasticKroneckerVertexCombiner extends Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex> {
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
