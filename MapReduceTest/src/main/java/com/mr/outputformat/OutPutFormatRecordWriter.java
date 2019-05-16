package com.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class OutPutFormatRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream foatguigu;
    FSDataOutputStream foother;

    public OutPutFormatRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        foatguigu = fs.create(new Path("/home/chyao/桌面/a.log"));
        foother = fs.create(new Path("/home/chyao/桌面/b.log"));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if(text.toString().contains("atguigu")){
            foatguigu.write(text.toString().getBytes());
        }else {
            foother.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(foatguigu);
        IOUtils.closeStream(foother);
    }
}
