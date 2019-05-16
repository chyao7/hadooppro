package com.mr.flowsum;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowParitioner extends Partitioner<Text,FlowSum> {


    @Override
    public int getPartition(Text text, FlowSum flowSum, int i) {
        String num = text.toString().substring(0,3);
        int partitooner =4 ;
        if("136".equals(num)){
            partitooner = 1;
        }else if("137".equals(num)){
            partitooner = 2;
        }else if("138".equals(num)){
            partitooner =0;
        }else {
            partitooner = 3;
        }
        return partitooner;
    }
}