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
public class FastStochasticKroneckerQuotaInputFormat extends InputFormat<FastStochasticKroneckerQuotaInputSplit, NullWritable> {

    Logger log = LoggerFactory.getLogger(FastStochasticKroneckerQuotaInputFormat.class);
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String strProbMatrix = conf.get(Constants.PROBABILITY_MATRIX);
        double[][] initatorMatrix = InitiatorMatrixUtils.parseInitiatorMatrix(strProbMatrix);
        double sumInitatorMatrix = InitiatorMatrixUtils.calculateMatrixSum(initatorMatrix);
        int n = Integer.parseInt(conf.get(Constants.N));

        long block_size = context.getConfiguration().getLong(Constants.BLOCK_SIZE, (int) Math.pow(2, 14));
        long totalEdges = (long) Math.pow(sumInitatorMatrix, n);

        log.info("Total edges: " + totalEdges);
        log.info("Block size: " + block_size);

        double rawNumberOfSplits = ((double) totalEdges)/block_size;
        log.info("Raw number of splits: " + rawNumberOfSplits);

        long numberOfSplits = (long) Math.ceil(rawNumberOfSplits);
        log.info("Number of splits: " + numberOfSplits);

        long quota = (long) Math.round(totalEdges / numberOfSplits);
        log.info("Quota: " + quota);

        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (long i = 0; i < numberOfSplits; i++) {
            log.info("adding a split for: " + quota + " edges");
            FastStochasticKroneckerQuotaInputSplit split = new FastStochasticKroneckerQuotaInputSplit(quota);
            splits.add(split);
        }

        return splits;
    }

    @Override
    public RecordReader<FastStochasticKroneckerQuotaInputSplit, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FastStochasticKroneckerQuotaRecordReader recordReader = new FastStochasticKroneckerQuotaRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }
    
}
