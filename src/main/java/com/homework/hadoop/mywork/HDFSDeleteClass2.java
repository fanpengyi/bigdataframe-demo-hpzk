package com.homework.hadoop.mywork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/19.
 */
public class HDFSDeleteClass2 {

    private static  final String  FILE_TYPE = "class";




    public static void main(String[] args) throws IOException {

        deleteFileType("/data/big-whale/storage");




    }


    public static void deleteFileType(String path) throws IOException {

        //1  初始化 HDFS 路径
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-80:9000");

        System.setProperty("HADOOP_USER_NAME","hadoop");


        FileSystem fs = FileSystem.get(conf);


        Path hdfsPath = new Path(path);

        FileStatus fileStatus = fs.getFileStatus(hdfsPath);


        if(fileStatus.isDirectory()){

            checkDirPath(hdfsPath,fs);

        }else{
            checkFilePath(hdfsPath,fs);
        }




    }

    private static void checkFilePath(Path hdfsPath, FileSystem fs) throws IOException {

        String name = hdfsPath.getName();

        int startIndex = name.length() - FILE_TYPE.length();

        int endIndex =  name.length();


        String fileSubffix = name.substring(startIndex, endIndex);

        if(FILE_TYPE.equals(fileSubffix)){
            System.out.println("有一致后缀的文件需要删除！"+name);
            //fs.delete(hdfsPath,true);
        }




    }

    private static void checkDirPath(Path hdfsPath, FileSystem fs) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(hdfsPath);


        for (FileStatus fileStatus : fileStatuses) {
            Path p = fileStatus.getPath();

            if(fileStatus.isDirectory()){

                checkDirPath(p,fs);

            }else{
                checkFilePath(p,fs);
            }


        }






    }









}
