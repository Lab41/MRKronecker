/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ndesai
 */
public class FastStochasticKroneckerQuotaRecordReader extends RecordReader<FastStochasticKroneckerQuotaInputSplit, NullWritable>{
    private FastStochasticKroneckerQuotaInputSplit inputSplit;
    Logger logger = LoggerFactory.getLogger(FastStochasticKroneckerQuotaRecordReader.class);
    TaskAttemptContext context;
    boolean nextKey = true;
    
    public FastStochasticKroneckerQuotaRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.inputSplit = (FastStochasticKroneckerQuotaInputSplit) split;
        this.context = context;

        if(logger.isDebugEnabled())
        {
            String logmessage =
                    String.format("QuotaRecordReader initialized for (%1$d) ", inputSplit.getQuota());

            logger.debug(logmessage);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return nextKey;
    }

    @Override
    public FastStochasticKroneckerQuotaInputSplit getCurrentKey() throws IOException, InterruptedException {
        nextKey = false;
        return (FastStochasticKroneckerQuotaInputSplit) inputSplit;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(nextKey = false)
            return 1f;
        else
            return 0f;
    }

    @Override
    public void close() throws IOException {
        logger.info("RecordReader being closed");
    }
    
}
