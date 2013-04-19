package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.junit.Test;

import static junit.framework.Assert.*;

/**
 * @author kramachandran
 */
public class StochasticKorneckerDriverTest {

    @Test
    public void testParseArgs() throws Exception {
        String testArgs = " /blah/blah 2 0.0 0.1 0.2 0.3 0.4";
        String[] args = testArgs.split(" ");
        StochasticKorneckerDriver driver = new StochasticKorneckerDriver();

        assertEquals(driver.parseArgs(args), true);

        assertEquals(driver.n, 2);
        assertEquals(driver.outputPath, "/blah/blah");
        assertEquals(driver.initiator,"0.0 0.1 0.2 0.3 0.4" );


    }

    @Test
    public void testFailParseArgs() throws Exception {
        String badArgs = " /blah/blah 2 0.0 0.1 0.2 0.3";
        String[] args = badArgs.split(" ");
        StochasticKorneckerDriver driver = new StochasticKorneckerDriver();
        assertEquals(driver.parseArgs(args), false);



    }
}
