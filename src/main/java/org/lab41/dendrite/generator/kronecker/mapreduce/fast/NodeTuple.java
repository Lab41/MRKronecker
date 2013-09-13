package org.lab41.dendrite.generator.kronecker.mapreduce.fast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * A serializable, comparable MapReduce key type representing
 * a directed edge. This class is a wrapper around a pair of long
 * vertex IDs; comparison is lexicographic.
 * 
 * @author ndesai
 */
public class NodeTuple implements WritableComparable<NodeTuple> {
    private long tail;
    private long head;

    public NodeTuple() {
 
    }
    
    /**
     * Constructs a NodeTuple representing a directed edge from tail
     * to head.
     * @param tail
     * @param head 
     */
    public NodeTuple(long tail, long head) {
        this.tail = tail;
        this.head = head;
    }
    
    /**
     * Sets this tuple's head and tail.
     */
    public void set(long tail, long head) {
        this.tail = tail;
        this.head = head;
    }
    
    public long getTail() {
        return tail;
    }
    
    public long getHead() {
        return head;
    }
    
    /**
     * Writes this NodeTuple to the given DataOutput.
     * @param out
     * @throws IOException 
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(tail);
        out.writeLong(head);
    }

    public void readFields(DataInput in) throws IOException {
        tail = in.readLong();
        head = in.readLong();
    }

    /**
     * Compares this NodeTuple to the given NodeTuple t lexicographically.
     * If the two tails are unequal, returns the result of comparing this
     * NodeTuple's head to t's; if they are equal,
     * returns the result of comparing this NodeTuple's tail to t's.
     * @param t
     * @return 
     */
    public int compareTo(NodeTuple t) {
        int tailDiff = compareLongs(this.tail,t.tail);
        return (tailDiff != 0) ? tailDiff : compareLongs(this.head,t.head);
    }
    
    private static int compareLongs(long a, long b) {
        return Long.valueOf(a).compareTo(b);
    }
}
