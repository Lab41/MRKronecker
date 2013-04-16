package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class LongSequenceRecordReader extends RecordReader<LongWritable, NullWritable>{

    private long startInterval;
    private long endInterval;
    private long currentKey;

    private LongSequenceGenerator generator;


    public LongSequenceRecordReader(LongSequenceGenerator generator) {
        //To change body of created methods use File | Settings | File Templates.
    }

    /**
     * {@inheritDoc}
     */
public void initalize(LongSequenceInputSplit split, TaskAttemptContext context)
    {
        startInterval = split.getStartInterval();
        startInterval = split.getEndInterval();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        initalize((LongSequenceInputSplit) split, context);
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
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        LongWritable retVal =new LongWritable(generator.generate(currentKey));
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
        //Does that +1 makes sense? Or will it all wash out for very large n?
        //probably washes out
        return (currentKey-startInterval) / (endInterval - startInterval +1);  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
