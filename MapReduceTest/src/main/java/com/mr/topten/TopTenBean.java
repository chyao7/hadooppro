package com.mr.topten;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopTenBean extends TopTenMapper implements WritableComparable<TopTenBean> {

    private int down;
    private int up;
    private int sum;

    public TopTenBean() {
        super();
    }

    public TopTenBean(int down, int up) {
        super();
        this.down = down;
        this.up = up;
        this.sum = up + down;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(down);
        dataOutput.writeInt(up);
        dataOutput.writeInt(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.down = dataInput.readInt();
        this.up = dataInput.readInt();
        this.sum = dataInput.readInt();
    }

    public int getDown() {
        return down;
    }

    public void setDown(int down) {
        this.down = down;
    }

    public int getUp() {
        return up;
    }

    public void setUp(int up) {
        this.up = up;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return  sum+"";
    }


    @Override
    public int compareTo(TopTenBean o) {
        int result;
        if (this.sum >o.getSum()){
            result = -1;
        }else if(this.sum<o.getSum()){
            result = 1;
        }else {
            result = 0;
        }
        return result;
    }
}
