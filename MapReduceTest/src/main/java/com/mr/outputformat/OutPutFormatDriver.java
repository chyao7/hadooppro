package com.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutPutFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OutPutFormatDriver.class);

        job.setMapperClass(OutPutFormatMapper.class);
        job.setReducerClass(OutputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(OutPutFormat.class);

        FileInputFormat.setInputPaths(job,new Path("/home/chyao/桌面/outputformat"));
        FileOutputFormat.setOutputPath(job, new Path("/home/chyao/桌面/outputformat2"));

        job.submit();
    }
}
