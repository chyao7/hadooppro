package com.mr.wrotablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReducer extends Reducer<Flowbean, Text,Text,Flowbean> {

    @Override
    protected void reduce(Flowbean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text valeue:values){
            context.write(valeue,key);
        }
    }
}
