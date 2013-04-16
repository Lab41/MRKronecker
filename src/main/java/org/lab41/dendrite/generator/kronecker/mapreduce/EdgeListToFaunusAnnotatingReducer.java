package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class convert an edge list to a Faunus vertex.
 * <p>
 * This job expects a k,v pair consisting of: (source id, dest id).
 * From this the job will construct a Faunus vertex and write that out to
 *
 * @author kramachandran
 */
public class EdgeListToFaunusAnnotatingReducer extends Reducer<LongWritable, LongWritable, FaunusVertex, NullWritable>
{


}
