package com.mr.wrotablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flowbean implements WritableComparable<Flowbean> {


    private long update;
    private long downdate;
    private long sumdata;

    public Flowbean() {
        super();
    }

    public Flowbean(long update, long downdate) {
        super();
        this.update = update;
        this.downdate = downdate;
        sumdata = update+downdate;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(update);
        dataOutput.writeLong(downdate);
        dataOutput.writeLong(sumdata);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            update = dataInput.readLong();
            downdate = dataInput.readLong();
            sumdata = dataInput.readLong();
    }

    @Override
    public int compareTo(Flowbean o) {
        int result;
        if(sumdata >o.getSumdata()){
            result =-1;
        }else if (sumdata<o.getSumdata()){
            result = 1;
        }else {
            result = 0;
        }
        return result;
    }

    public long getUpdate() {
        return update;
    }

    public void setUpdate(long update) {
        this.update = update;
    }

    public long getDowndate() {
        return downdate;
    }

    public void setDowndate(long downdate) {
        this.downdate = downdate;
    }

    public long getSumdata() {
        return sumdata;
    }

    public void setSumdata(long sumdata) {
        this.sumdata = sumdata;
    }

    @Override
    public String toString() {
        return update +"\t"+downdate+"\t"+sumdata;
    }
}
