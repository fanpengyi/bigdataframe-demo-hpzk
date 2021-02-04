package com.homework.hadoop.mywork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/18.
 */
public class HDFSCommonApi {

    static FileSystem fs;
    static Configuration conf;

    //初始化一个本地文件系统
    public static void initLocalFS() throws IOException {

        conf = new Configuration();

        fs = FileSystem.get(conf);

    }


    //代码设置访问 HDFS 文件系统

    public static void initHDfS() throws IOException, URISyntaxException {
        conf = new Configuration();
        //设置这个参数表示 从 HDFS 上获取一个分布式文件系统实例，否则默认表示从本地文件系统获取
        conf.set("fs.defaultFS", "hdfs://hadoop-80:9000");
        //设置客户端访问 hdfs 集群身份标识
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //
        fs = FileSystem.get(conf);

        //  或者使用
       // fs = FileSystem.get(new URI("hdfs://hadoop-80:9000"),conf);


    }
    // 从配置文件初始化 HDFS 文件系统

    public static void initHDFSWithConf() throws IOException {

        conf = new Configuration();

        //加载配置文件
        System.setProperty("HADOOP_USER_NAME","hadoop");
        conf.addResource("config/core-site.xml");
        conf.addResource("config/hdfs-site.xml");
        conf.addResource("config/mapred-site.xml");
        conf.addResource("config/yarn-site.xml");

        //参数优先级 ： 1 客户端代码设置 2 classpath 下文件 3 服务器上的配置文件

        conf.set("dfs.replication","2");
        conf.set("dfs.block.size","64m");


        fs = FileSystem.get(conf);


    }



    @Before
    public void init() throws IOException, URISyntaxException {

        initHDfS();

    }


    @After
    public void close(){

        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 显示指定文件夹下的 所有文件
     */

    @Test
    public void testListFiles() throws IOException {
        //param1 路径 ，param2 是否递归
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/data/big-whale/storage/fs"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("路径："+fileStatus.getPath()+"\t");
            System.out.println("文件名："+fileStatus.getPath().getName()+"\t");
            System.out.println("块大小："+fileStatus.getBlockSize()+"\t");
            System.out.println("权限："+fileStatus.getPermission()+"\t");
            System.out.println("副本数："+fileStatus.getReplication()+"\t");
            System.out.println("文件大小："+fileStatus.getLen()+"\t");

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation bl : blockLocations) {
                System.out.println("存储的块的个数:"+blockLocations.length+"   Block length:"+bl.getLength() + "Block offSet:  "+bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println("存储的主机地址："+host+"\t");
                }

                System.out.println();

            }


        }
        System.out.println("--------------------------------------");










    }

    /**
     * 上传文件
     */
    @Test
    public void testCopyFromLocal() throws Exception {
        // src : 要上传的文件所在的本地路径
        // dst : 要上传到hdfs的目标路径
        Path src = new Path("D:\\fpy\\study\\奈学\\homework\\hwhadoop1\\src\\main\\java\\com\\homework\\mr\\template\\flow\\flow.log");
        Path dst = new Path("/mrdata/flow/input/flow.log");
        fs.copyFromLocalFile(src, dst);
    }





}
