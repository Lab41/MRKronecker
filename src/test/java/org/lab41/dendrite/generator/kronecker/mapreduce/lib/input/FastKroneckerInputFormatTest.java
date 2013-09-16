package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Test;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author kramachandran
 */
public class FastKroneckerInputFormatTest {
    Logger log = LoggerFactory.getLogger(FastKroneckerInputFormatTest.class);

    @Test
    public void testGetSplits() throws Exception {
        FastKroneckerInputFormat fastKroneckerInputFormat = new FastKroneckerInputFormat();

        JobContext mockJobContext = mock(JobContext.class);
        JobConf mockJobConf = mock(JobConf.class);


        when(mockJobContext.getConfiguration()).thenReturn(mockJobConf);
        when(mockJobConf.get(Constants.N)).thenReturn("16");
        Long blockSize = (long) Math.pow(2, 8);
        when(mockJobConf.getLong(eq(Constants.BLOCK_SIZE), anyLong())).thenReturn(blockSize);
        when(mockJobConf.get(Constants.PROBABLITY_MATRIX)).thenReturn( "0.5, 0.5, 0.5, 0.5");

        List<InputSplit> inputSplits = fastKroneckerInputFormat.getSplits(mockJobContext);

        assertEquals(inputSplits.size(), Math.pow(2,8), 0.0001d);
        FastKroneckerInputSplit fastKroneckerInputSplit = (FastKroneckerInputSplit)inputSplits.get(0);
        assertEquals(fastKroneckerInputSplit.getQuota(),Math.pow(2,8), 0.0001d);
    }
}
