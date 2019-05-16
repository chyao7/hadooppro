package com.bigdata.hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsProcess {
    public static void main(String[] arg) throws InterruptedException, IOException, URISyntaxException {
//        DownloadFils();
//        RenameFiles();
//        Put();
//        OvervierFiles();
        testListStatus();
    }
    //文件删除
    public void delettest() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/sanguo/weiguo1"));
        fs.close();
        System.out.println("delete over");

    }

    //文件上传
    public static void Put() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("/home/chyao/桌面/renwu.txt"), new Path("/sanguo/shuguo/renwu2.txt"));
        fs.close();
    }
    //文件下载

    public static void DownloadFils() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf,"chyao");
        //fs.copyToLocalFile(new Path("/sanguo/shuguo/renwu2.txt"),new Path("/home/chyao/桌面"));
        fs.copyToLocalFile(false,new Path("/sanguo/shuguo/renwu2.txt"),
                new Path("/home/chyao/桌面/renwux.txt"));
        fs.close();
    }

    //文件更名
    public static void RenameFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf,"chyao");
        fs.rename(new Path("/sanguo/shuguo/renwu2.txt"),
                new Path("/sanguo/shuguo/renwu1.txt"));
        fs.close();
        System.out.println("OVER");

    }
    //查看文件的详情

    public static void OvervierFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf,"chyao");
        fs.listFiles(new Path("/"),true);
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getOwner());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations){
                String[] hosts = blockLocation.getHosts();
                for (String host:hosts){
                    System.out.println(host);
                }
            }
        }
        fs.close();
    }

    //判断文件状态
    public static void testListStatus() throws InterruptedException, URISyntaxException, IOException {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration, "atguigu");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/sanguo/"));
        for (FileStatus fileStatus : listStatus) {
        // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 3 关闭资源
        fs.close();
    }

}