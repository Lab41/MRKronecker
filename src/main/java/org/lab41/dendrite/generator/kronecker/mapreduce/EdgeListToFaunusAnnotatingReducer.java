package org.lab41.dendrite.generator.kronecker.mapreduce;

import cern.jet.random.Normal;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.UUID;

/**
 * This class convert an edge list to a Faunus vertex.
 * <p>
 * This job expects a k,v pair consisting of: (source id, dest id).
 * From this the job will construct a Faunus vertex and write that out to a sequence file.
 *
 * You can easily manipulate this to Reduce to wrtie to GraphSon or to any other format that Faunus writes to
 * by changing the ouptuformat in the driver. For example, see {@link com.thinkaurelius.faunus.formats.graphson.GraphSONOutputFormat}
 *
 * @author kramachandran
 */
public class EdgeListToFaunusAnnotatingReducer extends Reducer<LongWritable, LongWritable, NullWritable, FaunusVertex>
{

    protected void annotate(FaunusVertex vertex)
    {
          vertex.setProperty("uuid", UUID.randomUUID().toString());
          vertex.setProperty("name", UUID.randomUUID().toString());
          //TODO: Change the number and size of variables to be configurable.
          //Perhaps based on some configuration or XML file?

          //Add a bunch of longs
          for(int i = 0; i < 10; i++)
          {
                //TODO: change to the CERN random generator.. much faster
              vertex.setProperty("randLong" + Integer.toString(i), Math.random());
          }

          //Add a bunch of random strings
          for(int i = 0; i < 10; i++)
          {
              vertex.setProperty("randString" + Integer.toString(i) ,RandomStringUtils.randomAlphanumeric((int)Math.floor(Math.random() * 150)));
          }


    }

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //Create a new vertex
        FaunusVertex outVertex = new FaunusVertex(key.get());

        //Add all the properties
        annotate(outVertex);

        //Add all the edges
        for(LongWritable inVertex : values)
        {

            FaunusEdge edge = new FaunusEdge(key.get(), inVertex.get(), "RELATIONSHIP");
            outVertex.addEdge(Direction.OUT, edge);
        }


    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
