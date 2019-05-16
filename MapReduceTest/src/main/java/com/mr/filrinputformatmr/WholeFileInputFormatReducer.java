package com.mr.filrinputformatmr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WholeFileInputFormatReducer extends Reducer<Text, BytesWritable,Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Reducer<Text, BytesWritable,Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        for(BytesWritable value:values){
            context.write(key,value);
        }
    }
}
