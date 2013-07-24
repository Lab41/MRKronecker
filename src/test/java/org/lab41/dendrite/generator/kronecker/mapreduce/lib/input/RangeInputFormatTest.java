package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import java.util.List;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Test;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author ndesai
 */
public class RangeInputFormatTest {
    Logger log = LoggerFactory.getLogger(MatrixBlockInputFormatTest.class);
    
    @Test
    public void testSplits() throws Exception {
        RangeInputFormat inputFormat = new RangeInputFormat();
        
        JobContext mockJobContext = mock(JobContext.class);
        JobConf mockJobConf = mock(JobConf.class);
        
        when(mockJobContext.getConfiguration()).thenReturn(mockJobConf);
        when(mockJobConf.get(Constants.N)).thenReturn("16");
        long blockSize = 1 << 8;
        when(mockJobConf.getLong(eq(Constants.BLOCK_SIZE), anyLong())).thenReturn(blockSize);
        when(mockJobConf.get(Constants.PROBABILITY_MATRIX)).thenReturn("0.5, 0.5, 0.5, 0.5");
        
        List<InputSplit> inputSplits = inputFormat.getSplits(mockJobContext);

        assertEquals(inputSplits.size(), 1 << 8, 0.0001d);
        
        RangeInputSplit inputSplit = (RangeInputSplit) inputSplits.get(0);
        assertEquals(inputSplit.getStart(), 1, 0.0001d);
        assertEquals(inputSplit.getEnd(), 256);
        
        inputSplit = (RangeInputSplit) inputSplits.get(1);
        assertEquals(inputSplit.getStart(), 257);
        assertEquals(inputSplit.getEnd(), 512);
    }
    
    @Test
    public void testSplitDivision() throws Exception {
        RangeInputFormat inputFormat = new RangeInputFormat();
        
        JobContext mockJobContext = mock(JobContext.class);
        JobConf mockJobConf = mock(JobConf.class);
        
        when(mockJobContext.getConfiguration()).thenReturn(mockJobConf);
        when(mockJobConf.get(Constants.N)).thenReturn("4");
        long blockSize = 1 << 5;
        when(mockJobConf.getLong(eq(Constants.BLOCK_SIZE), anyLong())).thenReturn(blockSize);
        when(mockJobConf.get(Constants.PROBABILITY_MATRIX)).thenReturn("0.5, 0.5, 0.5, 0.5");
        
        List<InputSplit> inputSplits = inputFormat.getSplits(mockJobContext);

        assertEquals(inputSplits.size(), 1, 0.0001d);
        
        RangeInputSplit inputSplit = (RangeInputSplit) inputSplits.get(0);
        assertEquals(inputSplit.getStart(), 1, 0.0001d);
        assertEquals(inputSplit.getEnd(), 16);
    }
}