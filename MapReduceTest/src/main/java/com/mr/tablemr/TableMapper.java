package com.mr.tablemr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    TableBean t = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputsplit = (FileSplit) context.getInputSplit();
        name = inputsplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split("\t");

        if(name.startsWith("order")){


            t.setId(fields[0]);
            t.setPid(fields[1]);
            t.setNumber(Integer.parseInt(fields[2]));
            t.setName("");
            t.setFlag("order");
            k.set(fields[1]);


        }else {

            t.setId("");
            t.setPid(fields[0]);
            t.setNumber(0);
            t.setName(fields[1]);
            t.setFlag("Pid");
            k.set(fields[0]);

        }
        System.out.println(t);
        context.write(k,t);
    }
}
