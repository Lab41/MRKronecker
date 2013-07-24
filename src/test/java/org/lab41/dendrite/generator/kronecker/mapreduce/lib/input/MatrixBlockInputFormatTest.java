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
import org.junit.Ignore;
import static org.mockito.Mockito.*;

/**
 * @author kramachandran
 */
public class MatrixBlockInputFormatTest {

    Logger log = LoggerFactory.getLogger(MatrixBlockInputFormatTest.class);

    @Ignore
    @Test
    public void testGetSplits() throws Exception {
        MatrixBlockInputFormat matrixBlockInputFormat = new MatrixBlockInputFormat();

        JobContext mockJobContext = mock(JobContext.class);
        JobConf mockJobConf = mock(JobConf.class);


        when(mockJobContext.getConfiguration()).thenReturn(mockJobConf);
        when(mockJobConf.get(Constants.N)).thenReturn("16");
        Long blockSize = (long) Math.pow(2, 8);
        when(mockJobConf.getLong(eq(Constants.BLOCK_SIZE), anyLong())).thenReturn(blockSize);

        List<InputSplit> inputSplits = matrixBlockInputFormat.getSplits(mockJobContext);

        assertEquals(inputSplits.size(), 65536);


    }

    @Ignore
    @Test
    public void checkBlocks() throws Exception {
        MatrixBlockInputFormat matrixBlockInputFormat = new MatrixBlockInputFormat();

        JobContext mockJobContext = mock(JobContext.class);
        JobConf mockJobConf = mock(JobConf.class);


        when(mockJobContext.getConfiguration()).thenReturn(mockJobConf);
        when(mockJobConf.get(Constants.N)).thenReturn("9");
        Long blockSize = (long) Math.pow(2, 8);
        when(mockJobConf.getLong(eq(Constants.BLOCK_SIZE), anyLong())).thenReturn(blockSize);

        List<InputSplit> inputSplits = matrixBlockInputFormat.getSplits(mockJobContext);

        assertEquals(inputSplits.size(), 4);

        MatrixBlockInputSplit matrixBlockInputSplit0 = (MatrixBlockInputSplit) inputSplits.get(0);
        MatrixBlockInputSplit matrixBlockInputSplit1 = (MatrixBlockInputSplit) inputSplits.get(1);
        MatrixBlockInputSplit matrixBlockInputSplit2 = (MatrixBlockInputSplit) inputSplits.get(2);
        MatrixBlockInputSplit matrixBlockInputSplit3 = (MatrixBlockInputSplit) inputSplits.get(3);

        assertEquals(matrixBlockInputSplit0.startRowInterval, 1);
        assertEquals(matrixBlockInputSplit0.endRowInterval, 256);
        assertEquals(matrixBlockInputSplit0.startColInterval, 1);
        assertEquals(matrixBlockInputSplit0.endColInterval, 256);

        assertEquals(matrixBlockInputSplit1.startRowInterval, 1);
        assertEquals(matrixBlockInputSplit1.endRowInterval, 256);
        assertEquals(matrixBlockInputSplit1.startColInterval, 257);
        assertEquals(matrixBlockInputSplit1.endColInterval, 512);

        assertEquals(matrixBlockInputSplit2.startRowInterval, 257);
        assertEquals(matrixBlockInputSplit2.endRowInterval, 512);
        assertEquals(matrixBlockInputSplit2.startColInterval, 1);
        assertEquals(matrixBlockInputSplit2.endColInterval, 256);

        assertEquals(matrixBlockInputSplit3.startRowInterval, 257);
        assertEquals(matrixBlockInputSplit3.endRowInterval, 512);
        assertEquals(matrixBlockInputSplit3.startColInterval, 257);
        assertEquals(matrixBlockInputSplit3.endColInterval, 512);



    }
}
