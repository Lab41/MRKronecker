package org.lab41.dendrite.generators.models.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This input split represents a closed interval which is a continuous 
 * subset of the closed interval represented by {@LongSequenceInputFormat}
 * @author kramachandran
 */
public class LongSequenceInputSplit extends InputSplit implements Writable{
    long startInterval;
    long endInterval;
                              
    public LongSequenceInputSplit(long start, long end)
    {
        startInterval = start; 
        endInterval = end;
    }
    
    public long getStartInterval() {
        return startInterval;
    }

    public long getEndInterval() {
        return endInterval;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        //Pretty sure this should be +1 
        //since we are dealing with closed interval 
        
        return endInterval - startInterval + 1; 
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void write(DataOutput out) throws IOException {
       out.writeLong(startInterval);
       out.writeLong(endInterval);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startInterval = in.readLong();
        endInterval = in.readLong();
    }
}
