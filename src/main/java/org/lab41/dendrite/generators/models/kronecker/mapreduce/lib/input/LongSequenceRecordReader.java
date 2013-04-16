package org.lab41.dendrite.generators.models.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *  This record reader generats keys using a provided value generator. The value generator
 *
 *
 *
 * @author kramachandran
 */
public class LongSequenceRecordReader extends RecordReader<LongWritable, LongWritable>{

    public long startInterval;
    public long endInterval;
    public long currentKey;
    public long currentValue;


    public void initalize(LongSequenceInputSplit split, TaskAttemptContext context)
    {
        startInterval = split.getStartInterval();
        startInterval = split.getEndInterval();
    }
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        return true;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
