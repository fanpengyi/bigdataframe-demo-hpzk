package com.homework.hadoop.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFS_DELETE_CLASS {
	
	public static final String FILETYPE = "txt";
	
	public static void main(String[] args) throws Exception {
		
		new HDFS_DELETE_CLASS().rmrClassFile(new Path("/ddd"));
	}
	
	public void rmrClassFile(Path path) throws Exception{
		
		// 首先获取集群必要的信息，以得到FileSystem的示例对象fs
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata02:9000"), conf, "bigdata");
		
		// 首先检查path本身是文件夹还是目录
		FileStatus fileStatus = fs.getFileStatus(path);
		boolean directory = fileStatus.isDirectory();
		
		// 根据该目录是否是文件或者文件夹进行相应的操作
		if(directory){
			// 如果是目录
			checkAndDeleteDirectory(path, fs);
		}else{
			// 如果是文件，检查该文件名是不是FILETYPE类型的文件
			checkAndDeleteFile(path, fs);
		}
	}
	
	// 处理目录
	public static void checkAndDeleteDirectory(Path path, FileSystem fs) throws Exception{
		// 查看该path目录下一级子目录和子文件的状态
		FileStatus[] listStatus = fs.listStatus(path);
		for(FileStatus fStatus: listStatus){
			Path p = fStatus.getPath();
			// 如果是文件，并且是以FILETYPE结尾，则删掉，否则继续遍历下一级目录
			if(fStatus.isFile()){
				checkAndDeleteFile(p, fs);
			}else{
				checkAndDeleteDirectory(p, fs);
			}
		}
	}
	
	// 檢查文件是否符合刪除要求，如果符合要求則刪除，不符合要求则不做处理
	public static void checkAndDeleteFile(Path path, FileSystem fs) throws Exception{
		String name = path.getName();
		System.out.println(name);
		/*// 直接判断有没有FILETYPE这个字符串,不是特别稳妥，并且会有误操作，所以得判断是不是以FILETYPE结尾
		if(name.indexOf(FILETYPE) != -1){
			fs.delete(path, true);
		}*/
		// 判断是不是以FILETYPE结尾
		int startIndex = name.length() - FILETYPE.length();
		int endIndex = name.length();
		// 求得文件后缀名
		String fileSuffix = name.substring(startIndex, endIndex);
		if(fileSuffix.equals(FILETYPE)){
			fs.delete(path, true);
		}
	}
}
