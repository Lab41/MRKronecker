package org.lab41.dendrite.generator.kronecker.mapreduce;

import cern.jet.random.Binomial;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * This mapper will generate a <u>directed</u> graph with no using self loops using Kornecker Multiplication.
 *  <p>
 * More specifically, this mapper will generate all outgoing edges for given node, upon each invocation.
 * For example, if this mapper is invoked with <LongWritable(5), NullWritable> then the map function will generate
 * all edges emanating from that node (node id = 5). Each edge is emitted as a (Src, Dest) KV pair as  it is generated.
 *  <p>
 * The  Kornecker Graph Generation algorithm is explained in :
 *
 *      Leskovec, Jurij, Deepayan Chakrabarti, Jon Kleinberg, and Christos Faloutsos.
 *      "Realistic, mathematically tractable graph generation and evolution, using kronecker multiplication."
 *      In Knowledge Discovery in Databases: PKDD 2005, pp. 133-145. Springer Berlin Heidelberg, 2005.
 *      (http://www-cs.stanford.edu/people/jure/pubs/kronecker-pkdd05.pdf)
 *
 *
 * @author kramachandran
 */
public class StochasticKorneckerGeneratorMapper extends Mapper<LongWritable, NullWritable, LongWritable, LongWritable> {
    private Configuration configuration ;
    private int n;   // Where 2^n is the size of the graph.
    private float[][] probablity_matrix;
   private Logger logger = LoggerFactory.getLogger(StochasticKorneckerGeneratorMapper.class);

    /**
     * Expects the probablity matrix as a comma seperated string " t11, t12, t21, t22"
     * @param strProbabilityMartix
     * @return
     */
    protected float[][] parseProbabilityMartix(String strProbabilityMartix)
    {
        String[] splitMatrix = strProbabilityMartix.split(",");
        float[][] probabilityMatrix = new float[2][2];

        if(splitMatrix.length == 4)
        {
            for(int i=0, j=0; i < 4; i++)
            {
                float value = Float.parseFloat( splitMatrix[i]);
                probabilityMatrix[j][i % 2] = value;

                //increment j when i rolls past 1
                if( i % 2 == 1)
                   j++;
            }
        }
        else
        {
            throw new RuntimeException("the probablity matrix is not valid");
        }

        probablity_matrix = probabilityMatrix;
        return probabilityMatrix;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        n = Integer.getInteger(configuration.get(Constants.N));
        String strProbMatrix = configuration.get(Constants.PROBABLITY_MATRIX);
        probablity_matrix = parseProbabilityMartix(strProbMatrix)  ;

    }

    @Override
    protected void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        //Total number of nodes.
        long dimNodes;

        if (n < 64)
         dimNodes= (2<<n) -1;       // 2 ^ n
        else
         throw new RuntimeException("N is too large! Must be less than 64");

        long u = key.get();

        //Note the loops are one based here. We are starting the adjacency matrix from an index of 1,1

        for(int v = 0 ; v < dimNodes; v++)
        {
            if( u != v)
            {
                float p_uv = calculateProbabilityOfEdgeUV(u, v);
                int writeEdge = Binomial.staticNextInt(1, p_uv);
                if(writeEdge == 1)
                {
                    context.write(new LongWritable(u), new LongWritable(v));
                    context.getCounter("Graph Stats", "EdgeCount").increment(1l);
                    logger.info(String.format("Writing Edge: %1$d, %2$d", u, v ));
                }
            }
        }
        context.getCounter("Completed", "Nodes").increment(1l);
    }

    protected float calculateProbabilityOfEdgeUV(long u, int v) {
        float p_uv = 1; //probability
        for (int i = 0 ; i < n; i++)
        {
            float probability = getProbabilityForIteration(u, v, i, probablity_matrix);
            p_uv = probability * p_uv;
        }
        return p_uv;
    }

    /**
     * This function returns the appropriate parameter of the iteration matrix for a given
     * edge (u,v) & iteration i .
     * @param u -  row
     * @param v -  column
     * @param i - iteration step.
     * @return
     */
    protected float getProbabilityForIteration(long u, int v, int i, float[][] probaility_matrix) {

        //TODO: might want to precompute an array of 2^n
        long n_to_i = (2<< i) - 1; // 2^n;

        //calculating which of the probabilities to use in this step of the product
        //by figuring out which of the entries in the initiator matrix should be used.

        int prob_row = (int) (Math.floor((u-1)/ n_to_i) % 2);
        int prob_column = (int) (Math.floor((u-1)/n_to_i) % 2);

        logger.info(String.format("for (%1$d, %2$d) iteration %3$d using prob[%4$d, %5$d]",
                u, v, i, prob_row, prob_column));

        return probaility_matrix[prob_row][prob_column];
    }
}
