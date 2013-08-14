package org.lab41.dendrite.generator.kronecker.mapreduce;

import cern.jet.random.Uniform;
import com.tinkerpop.blueprints.Element;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The base mapper class for the Stochastic Kronecker graph generator, storing
 * the initiator matrix, the size of the graph, and some other bookkeeping
 * attributes for efficiency.
 * 
 * @author kramachandran
 */
public abstract class AnnotatingBaseReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    protected int numAnnotations = 0;
    protected int midAnnotations = 0;
    protected Uniform uniform = null;
    protected Configuration configuration;

    /**
     * Annotates the given element with k random floats, and ten random strings, 
     * where k is the number of annotations passed in at the command line.
     * @param vertex
     */
    protected void annotate(Element element) {
        //Add a bunch of longs
        for (int i = 0; i < midAnnotations; i++) {
            element.setProperty("randLong" + Integer.toString(i), uniform.nextDouble());
        }

        //Add a bunch of random strings
        for (int i = 0; i < numAnnotations-midAnnotations; i++) {
            element.setProperty("randString" + Integer.toString(i), RandomStringUtils.randomAlphanumeric((int) Math.floor(uniform.nextDouble() * 150)));
        }
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        numAnnotations = configuration.getInt(Constants.NUM_ANNOTATIONS, 5);
        midAnnotations = numAnnotations / 2;
        uniform = new Uniform(0, 1, 0);
    }
}
