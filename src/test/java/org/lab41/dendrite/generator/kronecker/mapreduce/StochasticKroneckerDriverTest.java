package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.junit.Test;

import static junit.framework.Assert.*;

/**
 * @author kramachandran
 */
public class StochasticKroneckerDriverTest {

    @Test
    public void testParseArgs() throws Exception {
        String testArgs = "/blah/blah 10 2 0.0 0.1 0.2 0.3";

        String[] args = testArgs.split(" ");
        StochasticKroneckerDriver driver = new StochasticKroneckerDriver();

        assertEquals(driver.parseArgs(args), true);

        assertEquals(driver.n, 2);
        assertEquals(driver.outputPath.toString(), "/blah/blah");
        assertEquals(driver.initiator,"0.0, 0.1, 0.2, 0.3" );
        assertEquals(driver.numAnnotations, 10);
    }

    @Test
    public void testFailParseArgs() throws Exception {
        String badArgs = "/blah/blah 2 0.0 0.1 0.2 0.3 x y z";
        String[] args = badArgs.split(" ");
        StochasticKroneckerDriver driver = new StochasticKroneckerDriver();
        assertEquals(driver.parseArgs(args), false);
    }
}
