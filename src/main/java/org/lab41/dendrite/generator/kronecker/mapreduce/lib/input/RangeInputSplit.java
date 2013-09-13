package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * An input split representing a range of vertices to be placed in the graph;
 * a wrapper around a pair of long values.
 * 
 * @author ndesai
 */
public class RangeInputSplit extends InputSplit implements Writable {
    long start;
    long end;

    public RangeInputSplit() {
    }
    
    public RangeInputSplit(long start, long end) {
        this.start = start;
        this.end = end;
    }
    
    public long getStart() {
        return start;
    }
    
    public long getEnd() {
        return end;
    }
    
    public void setStart(long start) {
        this.start = start;
    }
    
    public void setEnd(long end) {
        this.end = end;
    }
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        return end-start+1; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0]; //To change body of generated methods, choose Tools | Templates.
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(start);
        out.writeLong(end);
    }

    public void readFields(DataInput in) throws IOException {
        start = in.readLong();
        end = in.readLong();
    }
}
