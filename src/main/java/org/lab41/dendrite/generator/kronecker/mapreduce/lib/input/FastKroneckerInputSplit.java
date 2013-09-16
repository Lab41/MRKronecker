package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is a a special key class it contains three longs.
 * This enables the input format to tell the mapper in addition to outputting a faunus
 * vertex for each edge, to output a set of empty nodes [startNode, endNode]
 * This ensures that isolated nodes are represented.
 *
 * @author kramachandran
 */
public class FastKroneckerInputSplit extends InputSplit implements Writable {
    long quota;
    long startNode;
    long endNode;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(quota);
        out.writeLong(startNode);
        out.writeLong(endNode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        quota = in.readLong();
        startNode = in.readLong();
        endNode = in.readLong();
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return quota;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public long getQuota() {
        return quota;
    }

    public void setQuota(long quota) {
        this.quota = quota;
    }

    public long getStartNode() {
        return startNode;
    }

    public void setStartNode(long startNode) {
        this.startNode = startNode;
    }

    public long getEndNode() {
        return endNode;
    }

    public void setEndNode(long endNode) {
        this.endNode = endNode;
    }

    public FastKroneckerInputSplit() {
    }

    public FastKroneckerInputSplit( long startNode, long endNode, long quota) {
        this.quota = quota;
        this.startNode = startNode;
        this.endNode = endNode;
    }
}
