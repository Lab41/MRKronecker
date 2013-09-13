package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *  This record reader generats keys using a provided value generator. The value generator
 *
 *
 *
 * @author kramachandran
 */
public class LongSequenceRecordReader extends RecordReader<LongWritable, NullWritable>{

    private long startInterval;
    private long endInterval;
    private long currentKey;
    Logger logger = LoggerFactory.getLogger(LongSequenceRecordReader.class);



    public LongSequenceRecordReader() {

    }



    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        startInterval =((LongSequenceInputSplit) split).getStartInterval();
        endInterval = ((LongSequenceInputSplit)split).getEndInterval();
        currentKey = startInterval;
        logger.info("RecordReader Initialize start " + startInterval + "end " + endInterval + " current" + currentKey);
    }

    /**
     * It is assumed that the LongSequenceGenrator used by this function be able to map any Long
     * to some value. So this function will always return true.
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentKey <= endInterval)
        {
            return  true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        LongWritable retVal =new LongWritable(currentKey);
        currentKey +=1;
        return retVal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        float progress = 0.0f;
        if(endInterval == startInterval)
        {
            return 0f;
        }
        else
        {
            progress = Math.min(1.0f, (currentKey-startInterval) / (float)(endInterval - startInterval));
            logger.info("progress: " + progress);
            return progress;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        logger.info("CLose being called");
        //To change body of implemented methods use File | Settings | File Templates.
    }


}
