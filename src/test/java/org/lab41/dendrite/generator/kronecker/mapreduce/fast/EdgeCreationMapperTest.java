package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.lab41.dendrite.generator.kronecker.mapreduce.fast.EdgeCreationMapper.ProbabilityAndPair;

/**
 *
 * @author ndesai
 */
public class EdgeCreationMapperTest {
    
    @Test
    public void testBuildProbabilityVector() {
        ArrayList<ProbabilityAndPair> pairs = new ArrayList<ProbabilityAndPair>();
        double[][] initiatorMatrix = {{0.5,0.0},
                                      {0.0,0.5}};
        EdgeCreationMapper.buildProbVector(initiatorMatrix,pairs);
        assertEquals(pairs.size(), 2);
        assertEquals(pairs.get(0).prob, 0.5, 0.0001d);
        assertEquals(pairs.get(1).prob, 1.0, 0.0001d);
    }
    
    @Test
    public void testGetRowColumnForProbability() {
        ArrayList<ProbabilityAndPair> pairs = new ArrayList<ProbabilityAndPair>();
        double[][] initiatorMatrix = {{0.5,0.0},
                                      {0.0,0.5}};
        EdgeCreationMapper.buildProbVector(initiatorMatrix,pairs);
        
        ProbabilityAndPair p = EdgeCreationMapper.getRowColumnForProbability(0.3, pairs);
        assertEquals(p.col, 0); assertEquals(p.row, 0);
        
        p = EdgeCreationMapper.getRowColumnForProbability(0.7, pairs);
        assertEquals(p.col, 1); assertEquals(p.row, 1);
    }
}