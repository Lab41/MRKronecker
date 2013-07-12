package org.lab41.dendrite.generator.kronecker.mapreduce;

/**
 * A collection of configuration strings for Kronecker MapReduce jobs.
 * 
 * @author kramachandran
 */
public class Constants {
    public static final String CHECK_FOR_CONFLICTS = "true";
    public static String N = "kronecker.N";  // Where 2^N is the size of the graph
    public static String PROBABILITY_MATRIX = "kronecker.probability_matrix";

    //the block size should be a power of 2- it controls how many nodes are sent to each mapper.
    public static String BLOCK_SIZE = "kronecker.block_size";
}
