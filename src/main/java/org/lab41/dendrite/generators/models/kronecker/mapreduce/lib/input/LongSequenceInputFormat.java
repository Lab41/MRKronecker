package org.lab41.dendrite.generators.models.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

/**
 * @author kramachandran
 */
public class LongSequenceInputFormat implements InputFormat<LongWritable, LongWritable> {



    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
