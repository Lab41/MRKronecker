package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author kramachandran
 */
public class MatrixBlockRecordReader extends RecordReader<MatrixBlockInputSplit, NullWritable>
{
    private MatrixBlockInputSplit inputSplit;
    Logger logger = LoggerFactory.getLogger(MatrixBlockRecordReader.class);
    TaskAttemptContext context;
    boolean nextKey = true;

    public MatrixBlockRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.inputSplit = (MatrixBlockInputSplit) split;
        this.context = context;

        if(logger.isDebugEnabled())
        {
            String logmessage =
                    String.format("RecordReader initialized for (%1$d, %2$d) - (%3$d,%4$d), ",
                            inputSplit.startRowInterval, inputSplit.startColInterval,
                            inputSplit.endRowInterval, inputSplit.endColInterval);

            logger.debug(logmessage);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return nextKey;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MatrixBlockInputSplit getCurrentKey() throws IOException, InterruptedException {
        nextKey = false;
        return (MatrixBlockInputSplit) inputSplit;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
       float progress = 0.0f;
        if(nextKey = false)
            return 1f;
        else
            return 0f;
    }

    @Override
    public void close() throws IOException {
        logger.info("cloase being called");
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
