package com.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowSum,Text, FlowSum> {


    long upflow = 0;
    long downflow = 0;

    @Override
    protected void reduce(Text key, Iterable<FlowSum> values, Context context) throws IOException, InterruptedException {



        for(FlowSum value:values){
            upflow += value.getUpflow();
            downflow += value.getDownflow();
        }
        FlowSum result = new FlowSum(upflow,downflow);

        context.write(key,result);
    }
}
