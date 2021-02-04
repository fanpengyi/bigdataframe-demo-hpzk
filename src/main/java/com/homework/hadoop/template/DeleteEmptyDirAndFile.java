package com.homework.hadoop.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**  
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年7月3日 下午6:57:20 
 * 
 * 描述:  删除指定文件夹下的空文件夹 和 空文件
 */
public class DeleteEmptyDirAndFile {
	
	static FileSystem fs = null;

	public static void main(String[] args) throws Exception {
		
		initFileSystem();

//		 创建测试数据
//		makeTestData();

		// 删除测试数据
//		deleteTestData();

		// 删除指定文件夹下的空文件和空文件夹
		deleteEmptyDirAndFile(new Path("/aa"));
	}
	
	/**
	 * 删除指定文件夹下的 空文件 和 空文件夹
	 * @throws Exception 
	 */
	public static void deleteEmptyDirAndFile(Path path) throws Exception {
		
		//当是空文件夹时
		FileStatus[] listStatus = fs.listStatus(path);
		if(listStatus.length == 0){
			fs.delete(path, true);
			return;
		}
		
		// 该方法的结果：包括指定目录的  文件 和 文件夹
		RemoteIterator<LocatedFileStatus> listLocatedStatus = fs.listLocatedStatus(path);
		
		while (listLocatedStatus.hasNext()) {
			LocatedFileStatus next = listLocatedStatus.next();

			Path currentPath = next.getPath();
			// 获取父目录
			Path parent = next.getPath().getParent();
			
			// 如果是文件夹，继续往下遍历，删除符合条件的文件（空文件夹）
			if (next.isDirectory()) {
				
				// 如果是空文件夹
				if(fs.listStatus(currentPath).length == 0){
					// 删除掉
					fs.delete(currentPath, true);
				}else{
					// 不是空文件夹，那么则继续遍历
					if(fs.exists(currentPath)){
						deleteEmptyDirAndFile(currentPath);
					}
				}
				
			// 如果是文件
			} else {
				// 获取文件的长度
				long fileLength = next.getLen();
				// 当文件是空文件时， 删除
				if(fileLength == 0){
					fs.delete(currentPath, true);
				}
			}
			
			// 当空文件夹或者空文件删除时，有可能导致父文件夹为空文件夹，
			// 所以每次删除一个空文件或者空文件的时候都需要判断一下，如果真是如此，那么就需要把该文件夹也删除掉
			int length = fs.listStatus(parent).length;
			if(length == 0){
				fs.delete(parent, true);
			}
		}
	}
	
	/**
	 * 初始化FileSystem对象之用
	 */
	public static void initFileSystem() throws Exception{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		fs = FileSystem.get(conf);
	}

	/**
	 * 创建 测试 数据之用
	 */
	public static void makeTestData() throws Exception {
		
		String emptyFilePath = "D:\\bigdata\\empty.txt";
		String notEmptyFilePath = "D:\\bigdata\\notEmpty.txt";

		// 空文件夹 和 空文件 的目录
		String path1 = "/aa/bb1/cc1/dd1/";
		fs.mkdirs(new Path(path1));
		fs.mkdirs(new Path("/aa/bb1/cc1/dd2/"));
		fs.copyFromLocalFile(new Path(emptyFilePath), new Path(path1));
		fs.copyFromLocalFile(new Path(notEmptyFilePath), new Path(path1));

		// 空文件 的目录
		String path2 = "/aa/bb1/cc2/dd2/";
		fs.mkdirs(new Path(path2));
		fs.copyFromLocalFile(new Path(emptyFilePath), new Path(path2));

		// 非空文件 的目录
		String path3 = "/aa/bb2/cc3/dd3";
		fs.mkdirs(new Path(path3));
		fs.copyFromLocalFile(new Path(notEmptyFilePath), new Path(path3));

		// 空 文件夹
		String path4 = "/aa/bb2/cc4/dd4";
		fs.mkdirs(new Path(path4));

		System.out.println("测试数据创建成功");
	}

	/**
	 * 删除 指定文件夹
	 * @throws Exception 
	 */
	public static void deleteTestData() throws Exception {
		boolean delete = fs.delete(new Path("/aa"), true);
		System.out.println(delete ? "删除数据成功" : "删除数据失败");
	}

}
