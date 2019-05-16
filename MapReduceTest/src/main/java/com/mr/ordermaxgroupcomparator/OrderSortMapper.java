package com.mr.ordermaxgroupcomparator;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] files = line.split(" ");
        OrderBean k = new OrderBean();
        k.setOrder_id(Integer.parseInt(files[0]));
        k.setPrice(Double.parseDouble(files[2]));

        //xiechu

        context.write(k,NullWritable.get());


    }
}
