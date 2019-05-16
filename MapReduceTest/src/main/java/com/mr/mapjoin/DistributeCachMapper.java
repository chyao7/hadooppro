package com.mr.mapjoin;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;


import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class DistributeCachMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    Text k = new Text();

    HashMap<String, String> pmap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] catchfiles = context.getCacheFiles();
        String path = catchfiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){

            //切割
            String[] filds = line.split("\t");
            pmap.put(filds[0],filds[1]);
        }

        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        String name = pmap.get(fields[1]);

        line = line +"\t"+ name;

        k.set(line);

        context.write(k,NullWritable.get());


    }
}
