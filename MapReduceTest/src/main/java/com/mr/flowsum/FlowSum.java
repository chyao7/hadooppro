package com.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowSum implements Writable {

    private long upflow;
    private long downflow;
    private long sumflow;

    public FlowSum() {
        super();
    }

    public FlowSum(long upflow, long downflow) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow + downflow;
    }

    //序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);


    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //必须要求和反序列化顺序一致
        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumflow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow +"\t" + sumflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }
}
