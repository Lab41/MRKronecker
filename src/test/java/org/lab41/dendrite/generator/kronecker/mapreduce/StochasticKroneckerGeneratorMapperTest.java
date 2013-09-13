package org.lab41.dendrite.generator.kronecker.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;


import java.util.List;

/**
 * @author kramachandran
 */
public class StochasticKroneckerGeneratorMapperTest {
    Mapper mapper;


    @Before
    public void setUp() throws Exception {
        mapper = new StochasticKroneckerGeneratorMapper();




    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testParseProbabilityMartix() throws Exception {
        StochasticKroneckerGeneratorMapper stochasticKroneckerGeneratorMapper = new StochasticKroneckerGeneratorMapper();
        float[][] probabilityMatrix = stochasticKroneckerGeneratorMapper.parseProbabilityMartix("1.1, 1.2, 2.1, 2.2");

        assertEquals(probabilityMatrix[0][0], 1.1f);
        assertEquals(probabilityMatrix[0][1], 1.2f);
        assertEquals(probabilityMatrix[1][0], 2.1f);
        assertEquals(probabilityMatrix[1][1], 2.2f);


    }

    @Test
    public void testMap() throws Exception {
        Configuration conf =  new Configuration();
        conf.set(Constants.N, "2");
        conf.set(Constants.PROBABLITY_MATRIX, " 0.25, 0.25, 0.25, 0.25");

        MapDriver<LongWritable, NullWritable, LongWritable, LongWritable> mapDriver
                = new MapDriver<LongWritable, NullWritable, LongWritable, LongWritable>();

        mapDriver.withConfiguration(conf);
        mapDriver.withInput(new LongWritable(2L), NullWritable.get());
        mapDriver.setMapper(mapper);


        List<Pair<LongWritable, LongWritable>> results = mapDriver.run();

        long nodes = mapDriver.getCounters().findCounter("Completed", "Nodes").getValue();
        long edges = mapDriver.getCounters().findCounter("Graph Stats", "Edges").getValue();
        assertEquals(1, nodes);
        assertEquals(3, edges);


    }

    @Test
    public void testCalculateProbabilityOfEdgeUV() throws Exception {

    }

    @Test
    public void testGetProbabilityForIteration() throws Exception {

    }
}
