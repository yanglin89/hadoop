package com.run.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * 使用java api操作hdfs文件系统
 * 1、第一步 创建Configuration
 * 2、第二步 创建FileSystem
 * 3、第三步 java api操作
 */
public class HDFSApp {

    public static final String HDFS_URI = "hdfs://hadoop000:8020";
    Configuration configuration = null;
    FileSystem fileSystem =null;

    @Before
    public void beforeStart() throws Exception{
        System.out.println("------------before start--------------");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_URI),configuration,"root");
    }

    @After
    public void afterOver(){
        configuration = null;
        fileSystem = null;
        System.out.println("------------after over--------------");
    }

    @Test
    public void testMkdir() throws Exception{
        Path path = new Path("/hdfsapi/test");
        boolean result = fileSystem.mkdirs(path);

        System.out.println(result);
    }

    @Test
    public void testText() throws Exception{
        Path path = new Path("/hdfs-test/README.txt");
        FSDataInputStream in = fileSystem.open(path);
        IOUtils.copyBytes(in,System.out,1024);
    }

    @Test
    public void testCreate() throws Exception{
        Path path = new Path("/hdfsapi/test/aa.txt");
        FSDataOutputStream out = fileSystem.create(path);
        out.writeUTF("hello hadoop , hello bigdata");
        out.flush();
        out.close();
    }


}
