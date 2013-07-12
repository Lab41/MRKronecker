package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.lab41.dendrite.generator.kronecker.mapreduce.InitiatorMatrixUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kramachandran
 */
public class FastKroneckerInputFormat extends InputFormat<FastKroneckerInputSplit, NullWritable> {

    Logger log = LoggerFactory.getLogger(FastKroneckerInputFormat.class);


    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String strProbMatrix = conf.get(Constants.PROBABILITY_MATRIX);
        double[][] initatorMatrix = InitiatorMatrixUtils.parseInitiatorMatrix(strProbMatrix);
        double sumInitatorMatrix = InitiatorMatrixUtils.calculateMatrixSum(initatorMatrix);
        int n = Integer.parseInt(conf.get(Constants.N));

        long block_size = context.getConfiguration().getLong(Constants.BLOCK_SIZE, (int) Math.pow(2, 14));
        long totalEdges = (long) Math.pow(sumInitatorMatrix, n);
        long startSequence = 1l;
        long endSequence = (long) Math.pow(2, n);

        log.info("Total edges: " + totalEdges);
        log.info("Interval: " + startSequence + "," + endSequence);
        log.info("Block size: " + block_size);

        double rawNumberOfSplits = ((double)(endSequence-startSequence+1))/(double)block_size;
        log.info("Raw number of splits: " + rawNumberOfSplits);


        long numberOfSplits = (long) Math.ceil(rawNumberOfSplits);
        log.info("Number of splits: " + numberOfSplits);

        long quota = (long) Math.round(totalEdges / numberOfSplits);
        log.info("Quota: " + quota);


        List<InputSplit> splits = new ArrayList<InputSplit>();

        for (long i = startSequence; i < endSequence; i += block_size) {
            long startInterval = i;
            long endInterval = i + block_size - 1;
            if (endInterval > endSequence) endInterval = endSequence;

            log.info("adding a split for: " + i + "," + endInterval);
            FastKroneckerInputSplit split = new FastKroneckerInputSplit(startInterval,endInterval,quota);
            splits.add(split);
        }

        return splits;

    }

    @Override
    public RecordReader<FastKroneckerInputSplit, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
       FastKroneckerRecordReader recordReader = new FastKroneckerRecordReader();

        //TODO: Figure out if the initalize really needs to be here.
        recordReader.initialize(split, context);
        return recordReader;

    }
}
