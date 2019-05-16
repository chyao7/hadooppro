package com.mr.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//Reducer key,value为map阶段的kv
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    int sum;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //求和

        sum = 0;
        for (IntWritable value:values){
            sum += value.get();
        }
        v.set(sum);
        System.out.println(v.get());
        context.write(key, v);
    }
}

