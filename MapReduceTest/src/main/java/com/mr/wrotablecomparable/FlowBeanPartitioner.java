package com.mr.wrotablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//继承MAPPER输出的类型
public class FlowBeanPartitioner extends Partitioner<Flowbean, Text> {
    @Override
    public int getPartition(Flowbean flowbean, Text text, int i) {

        String num = text.toString().substring(0,3);
        int partationer = 4;


        if("139".equals(num)) {
            partationer =0;
        }else if("138".equals(num)){
            partationer =1;
        }else{
            partationer = 2;
        }

        return partationer;
    }
}

