package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author kramachandran
 */
public class FastKroneckerRecordReader extends RecordReader<FastKroneckerInputSplit, NullWritable>
{
    private FastKroneckerInputSplit inputSplit;
    Logger logger = LoggerFactory.getLogger(FastKroneckerRecordReader.class);
    TaskAttemptContext context;
    boolean nextKey = true;

    public FastKroneckerRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.inputSplit = (FastKroneckerInputSplit) split;
        this.context = context;

        if(logger.isDebugEnabled())
        {
            String logmessage =
                    String.format("RecordReader initialized for (%1$d, %2$d) - (%3$d) ",
                            inputSplit.getStartNode(), inputSplit.getEndNode(),
                            inputSplit.getQuota());

            logger.debug(logmessage);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return nextKey;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FastKroneckerInputSplit getCurrentKey() throws IOException, InterruptedException {
        nextKey = false;
        return (FastKroneckerInputSplit) inputSplit;
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
        logger.info("close being called");
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
