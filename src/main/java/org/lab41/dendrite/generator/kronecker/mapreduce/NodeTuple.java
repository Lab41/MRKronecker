/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ndesai
 */
public class NodeTuple implements WritableComparable<NodeTuple> {
    private long tail;
    private long head;

    public NodeTuple() {
 
    }
    
    public NodeTuple(long tail, long head) {
        this.tail = tail;
        this.head = head;
    }
    
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
    
    public void write(DataOutput out) throws IOException {
        out.writeLong(tail);
        out.writeLong(head);
    }

    public void readFields(DataInput in) throws IOException {
        tail = in.readLong();
        head = in.readLong();
    }

    public int compareTo(NodeTuple t) {
        int tailDiff = compareLongs(this.tail,t.tail);
        return (tailDiff != 0) ? tailDiff : compareLongs(this.head,t.head);
    }
    
    private static int compareLongs(long a, long b) {
        return Long.valueOf(a).compareTo(b);
    }
}
