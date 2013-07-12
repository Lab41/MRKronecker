package org.lab41.dendrite.generator.kronecker.mapreduce;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.assertEquals;
import org.apache.hadoop.mrunit.types.Pair;
import org.lab41.dendrite.generator.kronecker.mapreduce.lib.input.MatrixBlockInputSplit;

/**
 * @author kramachandran
 */
public class StochasticKroneckerGeneratorMapperTest {
    Mapper mapper;
    Logger logger = LoggerFactory.getLogger(StochasticKroneckerFaunusVertexGeneratorMapper.class);


    @Before
    public void setUp() throws Exception {
        mapper = new StochasticKroneckerFaunusVertexGeneratorMapper();


    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testParseProbabilityMartix() throws Exception {
        StochasticKroneckerFaunusVertexGeneratorMapper stochasticKroneckerGeneratorMapper = new StochasticKroneckerFaunusVertexGeneratorMapper();
        double[][] probabilityMatrix = InitiatorMatrixUtils.parseInitiatorMatrix("1.1, 1.2, 2.1, 2.2");

        assertEquals(probabilityMatrix[0][0], 1.1d);
        assertEquals(probabilityMatrix[0][1], 1.2d);
        assertEquals(probabilityMatrix[1][0], 2.1d);
        assertEquals(probabilityMatrix[1][1], 2.2d);


    }

    /**
     * 
     * @throws Exception 
     */
    @Test
    public void testMap() throws Exception {
        Configuration conf = new Configuration();
        conf.set(Constants.N, "2");
        conf.set(Constants.PROBABILITY_MATRIX, "1, 1, 1, 1");

        MapDriver<MatrixBlockInputSplit, NullWritable, LongWritable, LongWritable> mapDriver
                = new MapDriver<MatrixBlockInputSplit, NullWritable, LongWritable, LongWritable>();

        mapDriver.withConfiguration(conf);
        mapDriver.withInput(new MatrixBlockInputSplit(0, 3, 0, 3), NullWritable.get());
        mapDriver.setMapper(mapper);

        //List<Pair<LongWritable, LongWritable>> results = mapDriver.run();

        //long nodes = mapDriver.getCounters().findCounter("Completed", "Nodes").getValue();
        //long edges = mapDriver.getCounters().findCounter("Graph Stats", "Edges").getValue();
        //assertEquals(4, nodes);
        //assertEquals(16, edges);
    }


    @Test
    public void testGetProbabilityForIteration() throws Exception {
        StochasticKroneckerFaunusVertexGeneratorMapper stochasticKroneckerGeneratorMapper = new StochasticKroneckerFaunusVertexGeneratorMapper();
        double[][] probabilityMatrix = InitiatorMatrixUtils.parseInitiatorMatrix("0.11, 0.12, 0.21, 0.22");

        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 7, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 6, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 5, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 4, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 3, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 2, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 1, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(0, 0, 0, probabilityMatrix));
        
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 7, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 6, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 5, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 4, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 3, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 2, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 1, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(127, 127, 0, probabilityMatrix));
        
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 7, probabilityMatrix));
        assertEquals(0.22d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 6, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 5, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 4, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 3, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 2, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 1, probabilityMatrix));
        assertEquals(0.11d, stochasticKroneckerGeneratorMapper.getProbabilityForIteration(64, 64, 0, probabilityMatrix));

    }
}
