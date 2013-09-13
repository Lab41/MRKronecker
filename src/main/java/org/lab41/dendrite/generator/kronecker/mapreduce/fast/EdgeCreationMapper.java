package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.lab41.dendrite.generator.kronecker.mapreduce.InitiatorMatrixUtils;
import org.lab41.dendrite.generator.kronecker.mapreduce.StochasticKroneckerBaseMapper;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.QuotaInputSplit;

/**
 * Mapper class that operates on 
 * QuotaInputSplit-NullWritable pairs and outputs NodeTuple-NullWritable pairs 
 * representing edges in a stochastic Kronecker graph.
 * Leverages the Kron-Gen algorithm.
 * 
 * @author ndesai
 */
public class EdgeCreationMapper extends StochasticKroneckerBaseMapper<QuotaInputSplit, NullWritable, NodeTuple, NullWritable> {
    ArrayList<ProbabilityAndPair> cellProbabilityVector = new ArrayList<ProbabilityAndPair>();
    LongWritable nodeId = new LongWritable();
    NodeTuple edge = new NodeTuple();
    long dimNodes = 0l;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.cellProbabilityVector.clear();
        buildProbVector(probabilityMatrix);
        //TODO: remove assumption of 2x2 initiator matrix
        this.dimNodes = 1 << this.n;
    }
    
    /**
     * Class for elements of cellProbabilityVector. Triple storing row, column,
     * and cumulative probability for a cell of the initiator matrix. All
     * three parameters are immutable upon initialization.
     */
    private static class ProbabilityAndPair {
        public final long row;
        public final long col;
        public final double prob;

        public ProbabilityAndPair(long x,long y, double prob)
        {
            this.row = x;
            this.col = y;
            this.prob = prob;
        }
    }
    
    /**
     * Computes the cell associated with a given probability, to enable
     * recursive descent into the graph adjacency matrix.
     * 
     * @param probability 
     * @param cellProbabilityVector
     * @return 
     */
    private ProbabilityAndPair getRowColumnForProbability(double probability, ArrayList<ProbabilityAndPair> cellProbabilityVector) {
        int i = 0;
        while (probability > cellProbabilityVector.get(i).prob) {
            i++;
        }
        return cellProbabilityVector.get(i);
    }
    
    /**
     * Builds up cellProbabilityVector using the elements of initiatorMatrix.
     * <p>
     * All elements of initiatorMatrix must be between 0 and 1.
     * 
     * @param initiatorMatrix
     * @return 
     */
    protected void buildProbVector(double[][] initiatorMatrix)
    {
       cellProbabilityVector.clear();
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
                    cellProbabilityVector.add(probabilityAndPair);
                }
            }
        }
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

        for(int i = 0; i < this.n ; i++)
        {
            double probl = uniform.nextDouble();
            ProbabilityAndPair probabilityAndPair = getRowColumnForProbability(probl, this.cellProbabilityVector);

            //TODO: remove assumption of 2x2 initator matrix
            range /= 2;
            row += probabilityAndPair.row * range;
            col += probabilityAndPair.col * range;
        }
        
        edge.set(row, col);        
        context.write(edge, NullWritable.get());

        context.getCounter("Completed", "Edges Written").increment(1L);
    }

    @Override
    protected void map(QuotaInputSplit key, NullWritable value, Context context) throws IOException, InterruptedException {
        for(int i = 0; i < key.getQuota(); i++) {
            placeEdge(context);
        }
    }
}
