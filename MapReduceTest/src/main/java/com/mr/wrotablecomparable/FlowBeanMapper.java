package com.mr.wrotablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text,Flowbean,Text> {

    Text v = new Text();
    Flowbean k =new Flowbean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] files= line.split("\t");

        v.set(files[0]);

        long updata = Long.parseLong(files[1]);
        long downdata = Long.parseLong(files[2]);
        long sumdata = Long.parseLong(files[3]);

        k.setUpdate(updata);
        k.setDowndate(downdata);
        k.setSumdata(sumdata);;

//        System.out.println(k.getSumdata());

        context.write(k,v);



    }
}
