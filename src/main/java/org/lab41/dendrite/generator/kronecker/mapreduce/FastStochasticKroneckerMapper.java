package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.FastKroneckerInputSplit;
import sun.security.provider.NativePRNG;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This mapper implements the "fast" version of the Kronecker generator algorithm.
 * <p/>
 * Each mapper is given a block of edges to determine using the KronGen algorithm.
 * The output of the mapper is given a quota of edges to fill.
 *
 * @author kramachandran
 */
public class FastStochasticKroneckerMapper extends StochasticKroneckerBaseMapper<FastKroneckerInputSplit, NullWritable> {

    public ArrayList<ProbabilityAndPair> cellProbabilityVector = new ArrayList<ProbabilityAndPair>();
    public LongWritable nodeId = new LongWritable();
    protected long dimNodes = 0l;

    /**
     * Computes the cell associated with a given probability, to enable
     * recursive descent into the graph adjacency matrix.
     * 
     * @param probability 
     * @param cellProbabilityVector
     * @return 
     */
    public ProbabilityAndPair getRowColumnForProbability(double probability, ArrayList<ProbabilityAndPair> cellProbabilityVector) {
        int i = 0;
        while (probability > cellProbabilityVector.get(i).prob) {
            i++;
        }
        return cellProbabilityVector.get(i);
    }

    /**
     * Class for elements of cellProbabilityVector. Triple storing row, column,
     * and cumulative probability for a cell of the initiator matrix. All
     * three parameters are immutable upon initialization.
     */
    public class ProbabilityAndPair{
        public final long row;
        public final long col;
        public final Double prob;

        public ProbabilityAndPair(long x,long y, Double prob)
        {
            this.row = x;
            this.col = y;
            this.prob = prob;
        }
    }

    /**
     * Builds up cellProbabilityVector using the elements of initiatorMatrix.
     * <p>
     * All elements of initiatorMatrix must be between 0 and 1.
     * 
     * @param initiatorMatrix
     * @return 
     */
    protected ArrayList<ProbabilityAndPair> buildProbVector(double[][] initiatorMatrix)
    {
       ArrayList<ProbabilityAndPair> probabilityAndPairsList = new ArrayList<ProbabilityAndPair>();
       double cumulativeProb = 0d;
       double matrixSum = InitiatorMatrixUtils.calculateMatrixSum(initiatorMatrix);
        for(int i=0; i < initiatorMatrix.length; i++)
        {
            for(int k=0; k < initiatorMatrix[i].length; k++)
            {
                //check by zero to  handle case where a entry has a prob of zero.
                if(initiatorMatrix[i][k] > 0d)
                {
                    cumulativeProb += initiatorMatrix[i][k];
                    ProbabilityAndPair probabilityAndPair = new ProbabilityAndPair(i, k, cumulativeProb/matrixSum);
                    probabilityAndPairsList.add(probabilityAndPair);

                }

            }
        }
        return probabilityAndPairsList;

    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.cellProbabilityVector = buildProbVector(probablity_matrix);
        //TODO: remove assumption of 2x2 initiator matrix
        this.dimNodes = (long) Math.pow(2, this.n);

    }

    /**
     * Uses recursive descent to choose a cell of the adjacency matrix, then adds a Faunus edge
     * corresponding to this cell.
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    protected void placeEdge(Context context) throws IOException, InterruptedException {

        long range = dimNodes;
        long row = 0l;
        long col = 0l;

        for(int i = 0 ; i < this.n ; i++)
        {
            double probl = uniform.nextDouble();
            ProbabilityAndPair probabilityAndPair = getRowColumnForProbability(probl, this.cellProbabilityVector);

            //TODO: remove assumption of 2x2 initator matrix
            range /= 2;
            row += probabilityAndPair.row * range;
            col += probabilityAndPair.col * range;
        }

        faunusVertex = createVertex(row);
        faunusEdge = createEdge(row, col);
        faunusVertex.addEdge(Direction.OUT, faunusEdge);
        nodeId.set(row);
        context.write(nodeId, faunusVertex);

        context.getCounter("Completed", "Edges Written").increment(1L);
    }

    @Override
    protected void map(FastKroneckerInputSplit key, NullWritable value, Context context) throws IOException, InterruptedException {

        //Ensure that all nodes get created, even if they have no edges.
        //each mapper has a range of nodes its responsible for creating.
        for (long i = key.getStartNode(); i <= key.getEndNode(); i ++)
        {
            FaunusVertex node = createVertex(i);
            nodeId.set(i);
            context.write(nodeId, node);
        }

        //Create the edges
        //The total number of edges in the graph is calculated by the FastKroneckerInputFormat
        //Each mapper is given a quota of edges to place.
        for (int edges = 0 ; edges < key.getQuota(); edges++ )
        {
            placeEdge(context);
        }

    }
}
