package com.mr.tablemr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {


    private String id;
    private String pid;
    private int number;
    private String name;
    private String flag;

    public TableBean() {
        super();
    }

    public TableBean(String id, String pid, int number, String name, String flag) {
        super();
        this.id = id;
        this.pid = pid;
        this.number = number;
        this.name = name;
        this.flag = flag;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id+"\t"+name+"\t"+number;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(number);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        pid = dataInput.readUTF();
        number = dataInput.readInt();
        name = dataInput.readUTF();
        flag = dataInput.readUTF();
    }
}
