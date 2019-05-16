package com.mr.oneindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text,Text,Text,Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder s = new StringBuilder();
        for(Text value:values){
            s.append(value.toString().replace("\t", "-->")+"\t");
        }
        v.set(s.toString());
        context.write(key,v);
    }
}
