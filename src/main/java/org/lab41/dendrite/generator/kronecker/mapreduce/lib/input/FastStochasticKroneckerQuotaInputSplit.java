/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author ndesai
 */
public class FastStochasticKroneckerQuotaInputSplit extends InputSplit implements Writable {
    long quota;

    public FastStochasticKroneckerQuotaInputSplit(long quota) {
        this.quota = quota;
    }
    
    public long getQuota() {
        return quota;
    }
    
    public void setQuota(long quota) {
        this.quota = quota;
    }
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        return quota; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0]; //To change body of generated methods, choose Tools | Templates.
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(quota); //To change body of generated methods, choose Tools | Templates.
    }

    public void readFields(DataInput in) throws IOException {
        quota = in.readLong(); //To change body of generated methods, choose Tools | Templates.
    }
    
}
