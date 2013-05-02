package org.lab41.dendrite.generator.kronecker.mapreduce;

import cern.jet.random.Binomial;
import cern.jet.random.Uniform;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math.random.UniformRandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;


/**
 * This mapper will generate a <u>directed</u> graph with no using self loops using Kornecker Multiplication.
 *  <p/>
 * More specifically, this mapper will generate all outgoing edges for given node, upon each invocation.
 * For example, if this mapper is invoked with <LongWritable(5), NullWritable> then the map function will generate
 * all edges emanating from that node (node id = 5). Each edge is emitted as a (Src, Dest) KV pair as  it is generated.
 *  <p/>
 * The  Kornecker Graph Generation algorithm is explained in :
 *
 *      Leskovec, Jurij, Deepayan Chakrabarti, Jon Kleinberg, and Christos Faloutsos.
 *      "Realistic, mathematically tractable graph generation and evolution, using kronecker multiplication."
 *      In Knowledge Discovery in Databases: PKDD 2005, pp. 133-145. Springer Berlin Heidelberg, 2005.
 *      (http://www-cs.stanford.edu/people/jure/pubs/kronecker-pkdd05.pdf)
 * <p/>
 * As a quick note if edge ij exits that means the edge is directed from i to j.
 *
 * @author kramachandran
 */
public class StochasticKroneckerGeneratorMapper extends Mapper<LongWritable, NullWritable, NullWritable, FaunusVertex> {
    private Configuration configuration ;
    private int n = 0;   // Where 2^n is the size of the graph.
    private double[][] probablity_matrix = null;
   private Logger logger = LoggerFactory.getLogger(StochasticKroneckerGeneratorMapper.class);
    private Uniform uniform = null;

    /**
     * Expects the probablity matrix as a comma seperated string " t11, t12, t21, t22"
     * @param strProbabilityMartix
     * @return
     */
    protected double[][] parseProbabilityMartix(String strProbabilityMartix)
    {
        String[] splitMatrix = strProbabilityMartix.split(",");
        double[][] probabilityMatrix = new double[2][2];

        if(splitMatrix.length == 4)
        {
            probabilityMatrix[0][0] = Double.parseDouble(splitMatrix[0]);
            probabilityMatrix[0][1] = Double.parseDouble(splitMatrix[1]);
            probabilityMatrix[1][0] = Double.parseDouble(splitMatrix[2]);
            probabilityMatrix[1][1] = Double.parseDouble(splitMatrix[3]);

        }
        else
        {
            throw new RuntimeException("the probablity matrix is not valid");
        }

       // probablity_matrix = probabilityMatrix;
        return probabilityMatrix;
    }

    public double[][] getProbablity_matrix() {
        return probablity_matrix;
    }

    public void setProbablity_matrix(double[][] probablity_matrix) {
        this.probablity_matrix = probablity_matrix;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        String N = configuration.get(Constants.N) ;
        n = Integer.parseInt(N);
        String strProbMatrix = configuration.get(Constants.PROBABLITY_MATRIX);
        probablity_matrix = parseProbabilityMartix(strProbMatrix)  ;
        uniform = new Uniform(0, 1, 0);

    }


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
            vertex.setProperty("randString" + Integer.toString(i) , RandomStringUtils.randomAlphanumeric((int) Math.floor(Math.random() * 150)));
        }


    }

    protected FaunusVertex createVertex (long u)
    {
        //Create a new vertex
        FaunusVertex outVertex = new FaunusVertex(u);
        return outVertex;
    }

    protected FaunusEdge createEdge(long srcVertex, long destVertex)
    {
        FaunusEdge faunusEdge = new FaunusEdge(srcVertex, destVertex, "RELATIONSHIP");
        return faunusEdge;
    }

    @Override
    protected void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        //Total number of nodes.
        long dimNodes;
        long edgesWritten= 0;

        if (n < 64)
        {
         dimNodes= (long) Math.pow(2,n);       // 2 ^ n
            }
        else
         throw new RuntimeException("N is too large! Must be less than 64");

        long u = key.get();

        //create the vertex
        FaunusVertex vertex = createVertex(u);

        //annotate the vertex
        annotate(vertex);

        //Note the loops are one based here. We are starting the adjacency matrix from an index of 1,1
        for(long v = 1 ; v <= dimNodes; v++)
        {
            if( u != v)
            {
                //Note you do get different values if you use the uniform random generator
                //versus the Binonial genrator. No idea which is better.
                //TODO: write version of this that uses bionmial?
                double threshHold = uniform.nextDoubleFromTo(0,1);
                boolean placeEdge = placeEdge(u, v, n, threshHold);

                if(placeEdge)
                {
                    FaunusEdge faunusEdge = createEdge(u, v);
                    vertex.addEdge(Direction.OUT, faunusEdge);
                    edgesWritten++;
                }
                //context.getCounter("Graph Stats", "Edges Without Self Loops").increment(1l);

            }
            //context.getCounter("Graph Stats", "Edges").increment(1l);
        }
        context.write(NullWritable.get(), vertex);
        context.getCounter("Completed", "Nodes").increment(1l);
        context.getCounter("Completed", "EdgesWritten").increment(edgesWritten);
        context.progress();
    }


    /**
     *
     * @param u - row coordinate
     * @param v - column coordinate
     * @param n - N^n number of nodes. (where dim(InitMatrix) = N x N)
     * @return
     */
    protected boolean placeEdge(long u, long v, int n, double threshold) {
        double p_uv = 1d; //probability
        for (int i = 0 ; i < n; i++)
        {
            double probability = getProbabilityForIteration(u, v, i, probablity_matrix);
            p_uv = probability * p_uv;
            if(threshold > p_uv)
            {
                return false;
            }

        }
        return true;
    }

    /**
     *
     * @param u - row coordinate
     * @param v - column coordinate
     * @param n - N^n number of nodes. (where dim(InitMatrix) = N x N)
     * @return
     */
    protected double calculateProbabilityOfEdgeUV(long u, long v, int n) {
        double p_uv = 1d; //probability
        for (int i = 0 ; i < n; i++)
        {
            double probability = getProbabilityForIteration(u, v, i, probablity_matrix);
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
    protected double getProbabilityForIteration(long u, long v, int i, double[][] probaility_matrix) {
        //TODO Remove assumption of 2x2 generator matrix;
        long n_to_i = (long) Math.pow(2,i);

        //calculating which of the probabilities to use in this step of the product
        //by figuring out which of the entries in the initiator matrix should be used.

        int prob_row = (int) (Math.floor((u-1)/ n_to_i) % 2)  ;
        int prob_column = (int) (Math.floor((v-1)/n_to_i) % 2)  ;
        return probaility_matrix[prob_row][prob_column];
    }
}
