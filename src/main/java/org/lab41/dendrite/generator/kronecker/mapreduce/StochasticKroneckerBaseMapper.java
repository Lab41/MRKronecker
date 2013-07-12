package org.lab41.dendrite.generator.kronecker.mapreduce;

import cern.jet.random.Uniform;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The base mapper class for the Stochastic Kronecker graph generator, storing
 * the initiator matrix, the size of the graph, and some other bookkeeping
 * attributes for efficiency.
 * 
 * @author kramachandran
 */
public abstract class StochasticKroneckerBaseMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, LongWritable, FaunusVertex> {

    protected int n = 0;   // Where 2^n is the size of the graph.

    protected Uniform uniform = null;
    protected Configuration configuration;
    protected double[][] probabilityMatrix = null;

    protected FaunusVertex faunusVertex = new FaunusVertex();
    protected FaunusEdge faunusEdge = new FaunusEdge();

    org.apache.hadoop.io.LongWritable vertexId = new org.apache.hadoop.io.LongWritable();

    protected FaunusVertex createVertex(long u) {
        return faunusVertex.reuse(u);
    }


    protected FaunusEdge createEdge(long srcVertex, long destVertex) {
        return faunusEdge.reuse(-1l, srcVertex, destVertex, "RELATIONSHIP");
    }


    public double[][] getProbabilityMatrix() {
        return probabilityMatrix;
    }

    public void setProbabilityMatrix(double[][] probabilityMatrix) {
        this.probabilityMatrix = probabilityMatrix;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        String nString = configuration.get(Constants.N);
        n = Integer.parseInt(nString);
        String strProbMatrix = configuration.get(Constants.PROBABILITY_MATRIX);
        probabilityMatrix = InitiatorMatrixUtils.parseInitiatorMatrix(strProbMatrix);
        uniform = new Uniform(0, 1, 0);
    }


}
