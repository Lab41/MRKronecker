package org.lab41.dendrite.generator.kronecker.mapreduce;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.MatrixBlockInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;


/**
 * This mapper will generate a <u>directed</u> graph with no using self loops using Kornecker Multiplication.
 * <p/>
 * More specifically, this mapper will generate all outgoing edges for given node, upon each invocation.
 * For example, if this mapper is invoked with <LongWritable(5), NullWritable> then the map function will generate
 * all edges emanating from that node (node id = 5). Each edge is emitted as a (Src, Dest) KV pair as  it is generated.
 * <p/>
 * The  Kornecker Graph Generation algorithm is explained in :
 * <p/>
 * Leskovec, Jurij, Deepayan Chakrabarti, Jon Kleinberg, and Christos Faloutsos.
 * "Realistic, mathematically tractable graph generation and evolution, using kronecker multiplication."
 * In Knowledge Discovery in Databases: PKDD 2005, pp. 133-145. Springer Berlin Heidelberg, 2005.
 * (http://www-cs.stanford.edu/people/jure/pubs/kronecker-pkdd05.pdf)
 * <p/>
 * As a quick note if edge ij exits that means the edge is directed from i to j.
 *
 * @author kramachandran
 */
public class StochasticKroneckerFaunusVertexGeneratorMapper extends StochasticKroneckerBaseMapper<MatrixBlockInputSplit, NullWritable> {

    private Logger logger = LoggerFactory.getLogger(StochasticKroneckerFaunusVertexGeneratorMapper.class);



    @Override
    protected void map(MatrixBlockInputSplit key, NullWritable value, Context context) throws IOException, InterruptedException {
        //Total number of nodes.
        long dimNodes;

        if (n < 64) {
            dimNodes = (long) Math.pow(2, n);
        } else
            throw new RuntimeException("N is too large! Must be less than 64");

        long row_start = key.getStartRowInterval();
        long row_end = key.getEndRowInterval();

        long col_start = key.getStartColInterval();
        long col_end = key.getEndColInterval();

        /**************/
        //**TODO Make this DEBUG **/
        String logmessage =
                String.format("Mapper initialized for (%1$d, %2$d) - (%3$d,%4$d), ",
                        row_start, col_start,
                        row_end, col_end);
        logger.info(logmessage);

        /************/

        for (long u = row_start; u <= row_end; u++) {
            //create the vertex
            FaunusVertex vertex = createVertex(u);
            vertextId.set(u);

            //annotate the vertex

            long edgesWritten = 0;

            //Note the loops are one based here. We are starting the adjacency matrix from an index of 1,1
            for (long v = col_start; v <= col_end; v++) {

                if (u != v) {
                    //Note you do get different values if you use the uniform random generator
                    //versus the Binonial genrator. No idea which is better.
                    //TODO: write version of this that uses bionmial?
                    double threshHold = uniform.nextDoubleFromTo(0, 1);
                    boolean placeEdge = placeEdge(u, v, n, threshHold);

                    if (placeEdge) {
                        FaunusEdge faunusEdge = createEdge(u, v);
                        vertex.addEdge(Direction.OUT, faunusEdge);
                        edgesWritten++;
                    }
                }
            }

            context.getCounter("Completed", "EdgesWritten").increment(edgesWritten);
            context.write(vertextId, vertex);
        }

        context.getCounter("Completed", "Blocks Completed").increment(1l);

    }


    /**
     * @param u - row coordinate
     * @param v - column coordinate
     * @param n - N^n number of nodes. (where dim(InitMatrix) = N row N)
     * @return
     */
    protected boolean placeEdge(long u, long v, int n, double threshold) {
        double p_uv = 1d; //probability
        for (int i = 0; i < n; i++) {
            double probability = getProbabilityForIteration(u, v, i, probablity_matrix);
            p_uv = probability * p_uv;
            if (threshold > p_uv) {
                return false;
            }

        }
        return true;
    }


    /**
     * This function returns the appropriate parameter of the iteration matrix for a given
     * edge (u,v) & iteration i .
     *
     * @param u -  row
     * @param v -  column
     * @param i - iteration step.
     * @return
     */
    protected double getProbabilityForIteration(long u, long v, int i, double[][] probaility_matrix) {
        //TODO Remove assumption of 2x2 generator matrix;
        long n_to_i = (long) Math.pow(2, i);

        //calculating which of the probabilities to use in this step of the product
        //by figuring out which of the entries in the initiator matrix should be used.

        int prob_row = (int) (Math.floor((u - 1) / n_to_i) % 2);
        int prob_column = (int) (Math.floor((v - 1) / n_to_i) % 2);

        if(logger.isDebugEnabled())
        {
            String u_v = String.format("(u,v) : (%1$d , %2$d)", u, v);
            logger.debug(u_v);


            String prob_row_col = String.format("(prob_row,prob_col) : (%1$d , %2$d)", prob_row, prob_column);
            logger.debug(prob_row_col);
        }



        return probaility_matrix[prob_row][prob_column];
    }
}
