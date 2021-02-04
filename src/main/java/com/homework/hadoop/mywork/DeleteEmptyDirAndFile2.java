package com.homework.hadoop.mywork;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;


/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/19.
 */
public class DeleteEmptyDirAndFile2 {


    public static void main(String[] args){





    }

    public static void deleteEmptyDirAndFile(Path hdfsPath, FileSystem fs) throws IOException {


        if(fs.listStatus(hdfsPath).length == 0){
            fs.delete(hdfsPath,true);
        }


        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listLocatedStatus(hdfsPath);


        while(locatedFileStatusRemoteIterator.hasNext()){

            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();

            Path currentPath = next.getPath();
            Path parent = currentPath.getParent();


            if(next.isDirectory()){

                if(fs.listStatus(currentPath).length == 0){
                    fs.delete(currentPath,true);
                }else {

                    //不是空文件夹
                    if (fs.exists(currentPath)) {
                        deleteEmptyDirAndFile(currentPath ,fs);
                    }

                }


            }else{

                if(next.getLen() == 0){

                    fs.delete(currentPath,true);
                }
            }


            //判断父路径是否为空
            if(fs.listStatus(parent).length == 0){
                fs.delete(parent,true);
            }

        }

    }



}
