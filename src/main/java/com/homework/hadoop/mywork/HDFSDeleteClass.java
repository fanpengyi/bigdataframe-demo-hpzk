package com.homework.hadoop.mywork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 删除指定后缀名的文件
 *
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/18.
 */
public class HDFSDeleteClass {

    final static String FILETYPE = "class";


    // 递归删除 如果是目录则向下找 到文件删除，如果是文件则直接删除

    //1 查找指定目录下文件
    //2 如果是目录则 往下找
    //3 如果是文件则 删除

    public static void main(String[] args) throws Exception {


        rmrFileNameNew("/data/big-whale");

    }



    public static void rmrClassFile(String path) throws Exception {


        //1 获取文件系统
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://hadoop-80:9000");


        System.setProperty("HADOOP_USER_NAME","hadoop");

        FileSystem fs = FileSystem.get(conf);

        FileStatus fileStatus = fs.getFileStatus(new Path(path));


        if (fileStatus.isDirectory()) {
            //如果是目录
            checkAndDeleteDir(new Path(path), fs);

        } else {
            checkAndDeleteFile(new Path(path), fs);

        }


    }


    // 如果是目录
    private static void checkAndDeleteDir(Path path, FileSystem fs) throws Exception {

        //查看该 path 下目录和子目录的状态
        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {
            Path p = fileStatus.getPath();

            if (fileStatus.isDirectory()) {
                checkAndDeleteDir(p, fs);
            } else {
                checkAndDeleteFile(p, fs);
            }
        }

    }


    // 如果文件是以  fileType 结尾的 删除
    private static void checkAndDeleteFile(Path path, FileSystem fs) throws Exception {


        String fileName = path.getName();
        System.out.println("文件名称为：  " + fileName);

        int startIndex = fileName.length() - FILETYPE.length();

        int endIndex = fileName.length();

        String fileSuffix = fileName.substring(startIndex, endIndex);

        if(FILETYPE.equals(fileSuffix)){
            System.out.println("后缀名一致！"+ fileName);
            fs.delete(path,true);// true表示递归删除

        }

    }





    public static void rmrFileNameNew(String path) throws IOException {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://hadoop-80:9000");
        System.setProperty("HADOOP_USER_NAME","hadoop");


        FileSystem fs = FileSystem.get(conf);
        Path path1 = new Path(path);

        FileStatus fileStatuses = fs.getFileStatus(new Path(path));


        if(fileStatuses.isDirectory()){

            checkDir(path1,fs);

        }else{
            checkFile(path1,fs);
        }

        fs.close();


    }

    private static void checkFile(Path path, FileSystem fs) {

        String name = path.getName();

        int startIndex = name.length() - FILETYPE.length();

        int endIndex = name.length();


        String fileSuffix = name.substring(startIndex, endIndex);

        if(FILETYPE.equals(fileSuffix)){

            System.out.println("找到了后缀名相同的文件！，"+ name);
        }


    }

    private static void checkDir(Path path, FileSystem fs) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {

            Path p = fileStatus.getPath();

            if(fileStatus.isDirectory()){
                checkDir(p,fs);
            }else{
                checkFile(p,fs);
            }


        }




    }


}
