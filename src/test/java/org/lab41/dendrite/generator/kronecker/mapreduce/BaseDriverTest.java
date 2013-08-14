package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.junit.Test;
import org.lab41.dendrite.generator.kronecker.mapreduce.fast.EdgeCreationDriver;
import static org.junit.Assert.assertEquals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ndesai
 */
public class BaseDriverTest {
    Logger log = LoggerFactory.getLogger(BaseDriverTest.class);
    
    @Test
    public void testParseArgs() throws Exception {
        String testArgs = "/blah/blah 10 2 0.0 0.1 0.2 0.3";

        String[] args = testArgs.split(" ");
        BaseDriver driver = new EdgeCreationDriver();

        assertEquals(driver.parseArgs(args), true);

        assertEquals(driver.n, 2);
        assertEquals(driver.outputPath.toString(), "/blah/blah");
        assertEquals(driver.initiator,"0.0, 0.1, 0.2, 0.3" );
        assertEquals(driver.numAnnotations, 10);
    }

    @Test
    public void testFailParseArgs() throws Exception {
        String badArgs = "/blah/blah 2 0.0 0.1 0.2 0.3 (extra) (args)";
        String[] args = badArgs.split(" ");
        BaseDriver driver = new EdgeCreationDriver();
        assertEquals(driver.parseArgs(args), false);
    }
}