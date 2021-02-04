package com.homework.hadoop.mywork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/18.
 */
public class DeleteEmprtDirAndFile {


    static FileSystem fs;

    public static void main(String[] args) throws IOException {



        initFileSystem();

        makeTestData();


        deleteEmptyDirAndFiles(new Path("/aa"));

    }



    /**
     * 获取 文件系统
     */
    public static void initFileSystem() throws IOException {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://hadoop-80:9000");

        System.setProperty("HADOOP_USER_NAME","hadoop");

        fs = FileSystem.get(conf);
    }


    //准备空文件 与 空目录
    public static void makeTestData() throws IOException {


        String emptyFilePath = "D:\\bigdata\\empty.txt";
        String notEmptyFilePath = "D:\\bigdata\\noEmpty.txt";

        // 空文件夹 和 空文件 的目录  dd1 -- empty 删除
        String path1 = "/aa/bb1/cc1/dd1/";
        fs.mkdirs(new Path(path1));
        fs.mkdirs(new Path("/aa/bb1/cc1/dd2/"));
        fs.copyFromLocalFile(new Path(emptyFilePath), new Path(path1));
        fs.copyFromLocalFile(new Path(notEmptyFilePath), new Path(path1));

        // 空文件 的目录 --  dd2 删除
        String path2 = "/aa/bb1/cc2/dd2/";
        fs.mkdirs(new Path(path2));
        fs.copyFromLocalFile(new Path(emptyFilePath), new Path(path2));

        // 非空文件 的目录
        String path3 = "/aa/bb2/cc3/dd3";
        fs.mkdirs(new Path(path3));
        fs.copyFromLocalFile(new Path(notEmptyFilePath), new Path(path3));

        // 空 文件夹 -- dd4 删除
        String path4 = "/aa/bb2/cc4/dd4";
        fs.mkdirs(new Path(path4));

        System.out.println("测试数据创建成功");


    }

    // 删除指定目录下 空的目录和文件
    //1 先判断目录是否为空
    //2 不为空 逐级判断 是目录 - liststatus.length == 0
    //               是文件 - len == 0
    public static void deleteEmptyDirAndFile(Path path) throws IOException {
        //获取文件列表状态
        FileStatus[] fileStatuses = fs.listStatus(path);

        if (fileStatuses.length == 0) {
            fs.delete(path, true);
        }

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listLocatedStatus(path);

        //遍历当前文件夹下所有目录与文件
        while (locatedFileStatusRemoteIterator.hasNext()) {

            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();

            Path currentPath = next.getPath();
            Path parentPath = currentPath.getParent();

            //判断当前是不是目录
            if (next.isDirectory()) {

                //如果是空文件夹
                if (fs.listStatus(currentPath).length == 0) {
                    //删除
                    fs.delete(currentPath, true);
                } else {

                    //不是空文件夹
                    if (fs.exists(currentPath)) {
                        deleteEmptyDirAndFile(currentPath);
                    }

                }

            } else {
                //是文件
                //获取文件大小
                if (next.getLen() == 0) {

                    fs.delete(currentPath, true);
                }
            }
            //当空文件夹或空文件删除时 可能导致父文件夹为空
            //所以每次删除一个空文件夹或者空文件的时候判断一下 父文件夹是否为空

            long parentLength = fs.listStatus(path).length;

            if (parentLength == 0) {
                fs.delete(path, true);
            }


        }


    }





    public static void deleteEmptyDirAndFiles(Path path) throws IOException {

        FileStatus[] fileStatuses = fs.listStatus(path);

        if(fileStatuses.length == 0){
            fs.delete(path,true);
            System.out.println(path+":为空！");
        }


        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listLocatedStatus(path);


        while(locatedFileStatusRemoteIterator.hasNext()){

            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();

            Path currentPath = next.getPath();
            Path parent = currentPath.getParent();

            if(next.isDirectory()){

                if(fs.listStatus(currentPath).length == 0){
                    fs.delete(currentPath,true);
                }else{

                    if(fs.exists(currentPath)){
                        deleteEmptyDirAndFiles(currentPath);
                    }

                }

            }else{

                if(next.getLen() == 0){
                    fs.delete(currentPath,true);
                }
            }


            // 最后判断父目录是否为空 为空则删除父目录

            if(fs.listStatus(parent).length == 0){
                fs.delete(parent,true);

            }


        }















    }







}
