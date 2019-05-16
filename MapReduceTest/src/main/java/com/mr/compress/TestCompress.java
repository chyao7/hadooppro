package com.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import sun.awt.ComponentFactory;

import javax.print.attribute.standard.Compression;
import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //压缩
//        compress("/home/chyao/桌面/数据清洗/access_2013_05_30.log","org.apache.hadoop.io.compress.BZip2Codec");
        //解压缩
        decompress("/home/chyao/桌面/数据清洗/access_2013_05_30.log.bz2");
    }

    private static void decompress(String filename) throws IOException {

        //压缩方式检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec== null){
            System.out.println("can not decompress");
            return;
        }

        FileInputStream fis = new FileInputStream(new File(filename));
        CompressionInputStream cis = codec.createInputStream(fis);

        FileOutputStream fos = new FileOutputStream(new File(filename+".decode"));
        IOUtils.copyBytes(cis,fos,1024*1024*5,false);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }

    public static void compress(String filename, String method) throws IOException, ClassNotFoundException {

        //获取输入流

        FileInputStream fis = new FileInputStream(new File(filename));

        //获取输出流

        Class codeclass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeclass,new Configuration());
        FileOutputStream fos = new FileOutputStream(new File(filename+codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //流的对拷
        IOUtils.copyBytes(fis,cos,1024*1024*5,false);

        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
