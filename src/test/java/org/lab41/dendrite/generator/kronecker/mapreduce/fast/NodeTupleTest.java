package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author ndesai
 */
public class NodeTupleTest {
    
    @Test
    public void testComparison() {
        NodeTuple a = new NodeTuple(1,1);
        NodeTuple b = new NodeTuple(2,1);
        NodeTuple c = new NodeTuple(1,2);
        
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) == -a.compareTo(b));
        assertTrue(c.compareTo(a) > 0);
        assertTrue(c.compareTo(b) < 0);
    }
}