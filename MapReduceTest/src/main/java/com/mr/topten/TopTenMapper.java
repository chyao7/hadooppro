package com.mr.topten;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopTenMapper extends Mapper<LongWritable, Text,TopTenBean,Text> {


    private TreeMap<TopTenBean,Text> treeMap = new TreeMap<TopTenBean,Text>();
    private TopTenBean k;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        k = new TopTenBean(Integer.parseInt(fields[1]),Integer.parseInt(fields[2]));


        Text v = new Text();

        v.set(fields[0]);

        treeMap.put(k,v);
        if (treeMap.size()>10){
            treeMap.remove(treeMap.lastKey());
        }
    }



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<TopTenBean> bean = treeMap.keySet().iterator();
        while (bean.hasNext()){
            TopTenBean a = bean.next();
            context.write(a,treeMap.get(a));
        }
    }
}