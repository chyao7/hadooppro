package com.mr.topten;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopTenReducer extends Reducer<TopTenBean,Text,Text,TopTenBean> {


    @Override
    protected void reduce(TopTenBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value:values){
            context.write(value,key);
        }
    }
}
