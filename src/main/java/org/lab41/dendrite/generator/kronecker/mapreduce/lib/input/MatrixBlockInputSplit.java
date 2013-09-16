package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This input split represent a submatrix:
 *
 *  startRowInterval, startColInterval , .... , startRowInterval, endColInterval
 *  :
 *  :
 *  endRowInterval, startColInterval, ... , endRowInterval, endColInterval
 *
 * @author kramachandran
 */
public class MatrixBlockInputSplit extends InputSplit implements Writable {
    long startRowInterval =0l;
    long endRowInterval=0l;
    long startColInterval=0l;
    long endColInterval=0l;


    public MatrixBlockInputSplit(long startRowInterval, long endRowInterval, long startColInterval, long endColInterval) {

        this.startRowInterval = startRowInterval;
        this.endRowInterval = endRowInterval;
        this.startColInterval = startColInterval;
        this.endColInterval = endColInterval;


    }

    public MatrixBlockInputSplit() {
    }


    @Override
    public long getLength() throws IOException, InterruptedException {
        return ((endRowInterval-startRowInterval +1 ) * (endColInterval-startColInterval +1));
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(startRowInterval);
        out.writeLong(endRowInterval);
        out.writeLong(startColInterval);
        out.writeLong(endColInterval);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       startRowInterval = in.readLong();
       endRowInterval = in.readLong();
        startColInterval = in.readLong();
        endColInterval = in.readLong();
    }

    public long getStartRowInterval() {
        return startRowInterval;
    }

    public void setStartRowInterval(long startRowInterval) {
        this.startRowInterval = startRowInterval;
    }

    public long getEndRowInterval() {
        return endRowInterval;
    }

    public void setEndRowInterval(long endRowInterval) {
        this.endRowInterval = endRowInterval;
    }

    public long getStartColInterval() {
        return startColInterval;
    }

    public void setStartColInterval(long startColInterval) {
        this.startColInterval = startColInterval;
    }

    public long getEndColInterval() {
        return endColInterval;
    }

    public void setEndColInterval(long endColInterval) {
        this.endColInterval = endColInterval;
    }
}
