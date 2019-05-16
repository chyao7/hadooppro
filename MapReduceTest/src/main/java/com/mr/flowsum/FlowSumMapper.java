package com.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowSum> {

    FlowSum v = new FlowSum();
    Text k = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        k.set(words[0]);


        long upflow = Long.parseLong(words[words.length-3]);
        long downflow = Long.parseLong(words[words.length-2]);

        v.setUpflow(upflow);
        v.setDownflow(downflow);

        context.write(k, v);
    }
}
