package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This input format will emit a sequence of LongWriables values as Keys, and null writables as values.
 * <p/>
 * This input format is intiatlized with a start value, an end value, and a genertor functor.
 * The generator function is called for every long For evey long in the closed interval [start,end]
 * and the return value of that generator function is emited as key.
 * <p/>
 * By default this function will evenly divide the range [start,end] between the number of mappers set in
 * the JobConf using MRJobConfig.NUM_MAPS.  If this value is not set then we default to 1.
 *
 * @author kramachandran
 */
public class LongSequenceInputFormat extends InputFormat<LongWritable, NullWritable> {
    Long startSequence;
    Long endSequence;

    Logger log = LoggerFactory.getLogger(LongSequenceInputFormat.class);

    public LongSequenceInputFormat() {
        super();
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long chunksize;
        //TODO - change this to pull a start number, end number, and generator from conf.        function from the configuration
        int n = Integer.parseInt(conf.get(Constants.N));
        startSequence = 1l;
        endSequence = (long) Math.pow(2, n);

        log.info("Interval : " + startSequence + "," + endSequence);

        List<InputSplit> splits = new ArrayList<InputSplit>();
        Integer chunks = context.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);

        //TODO: add error checking to make sure startSequence is less than endSequence
        if(chunks != 1 )
        {
         chunksize = (endSequence - startSequence + 1) / chunks;
        }
        else
        {
            //We'll do 64,000  nodes to a mapper -- if the mappers produce less that
            //64M of data we probably have to up this.
            chunksize =(long) Math.pow(2, 14);    //approx 16k
        }

        for (long i = startSequence; i < endSequence; i += chunksize) {

            long startInterval = i;

            long endInterval = i + chunksize - 1;

            if(endInterval > endSequence)
                endInterval = endSequence;

            log.info("adding a split for :" + i + "," + endInterval);
            LongSequenceInputSplit split = new LongSequenceInputSplit(startInterval, endInterval);
            splits.add(split);
        }


        return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        log.info("creating record Reader");
        LongSequenceRecordReader recordReader = new LongSequenceRecordReader();
        recordReader.initialize((LongSequenceInputSplit) split, context);
        return recordReader;

    }
}
