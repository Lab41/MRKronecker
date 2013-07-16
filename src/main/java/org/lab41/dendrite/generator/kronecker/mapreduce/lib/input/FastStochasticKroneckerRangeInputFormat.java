/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.lab41.dendrite.generator.kronecker.mapreduce.InitiatorMatrixUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ndesai
 */
public class FastStochasticKroneckerRangeInputFormat extends InputFormat<FastStochasticKroneckerRangeInputSplit, NullWritable> {

    Logger log = LoggerFactory.getLogger(FastStochasticKroneckerRangeInputFormat.class);
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int n = Integer.parseInt(conf.get(Constants.N));

        long block_size = context.getConfiguration().getLong(Constants.BLOCK_SIZE, (int) Math.pow(2, 14));
        long startSequence = 1l;
        long endSequence = (long) Math.pow(2, n);
        
        log.info("Interval: " + startSequence + "," + endSequence);
        log.info("Block size: " + block_size);

        double rawNumberOfSplits = ((double)(endSequence-startSequence+1))/block_size;
        log.info("Raw number of splits: " + rawNumberOfSplits);

        long numberOfSplits = (long) Math.ceil(rawNumberOfSplits);
        log.info("Number of splits: " + numberOfSplits);

        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (long i = startSequence; i < endSequence; i += block_size) {
            long startInterval = i;
            long endInterval = i+block_size-1;
            if(endInterval > endSequence) endInterval = endSequence;
            
            log.info("adding a split for: vertices " + startInterval + " to " + endInterval);
            FastStochasticKroneckerRangeInputSplit split = new FastStochasticKroneckerRangeInputSplit(startInterval,endInterval);
            splits.add(split);
        }

        return splits;
    }

    @Override
    public RecordReader<FastStochasticKroneckerRangeInputSplit, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FastStochasticKroneckerRangeRecordReader recordReader = new FastStochasticKroneckerRangeRecordReader();

        recordReader.initialize(split, context);
        return recordReader;
    }
    
}
