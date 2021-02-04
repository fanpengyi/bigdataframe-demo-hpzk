package com.homework.hadoop.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFS_API_DEMO {

	public static FileSystem fs;
	public static Configuration conf;
	
	// 初始一个本地文件系统
	public static void initLocalFS() throws Exception{
		conf = new Configuration();
		// 利用FileSystem的自身携带的get方法获取FileSystem的一个实例
		fs = FileSystem.get(conf);
	}
	
	// 代码设置访问初始化一个HDFS文件系统示例对象
	public static void initHDFS() throws Exception{
		conf = new Configuration();
		// 设置了该参数表示从hdfs获取一个分布式文件系统的实例，否则默认表示获取一个本地文件系统的实例
		conf.set("fs.defaultFS", "hdfs://bigdata02:9000");
		// 设置客户端访问hdfs集群的身份标识
		System.setProperty("HADOOP_USER_NAME", "bigdata");
		// 利用FileSystem的自身携带的get方法获取FileSystem的一个实例
		fs = FileSystem.get(conf);
	}
	
	// 从配置文件初始化一个HDFS文件系统实例对象
	public static void initHDFSWithConf() throws Exception{
		/**
		 * 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
		 * 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址 new
		 * Configuration();的时候,它就会去加载jar包中的hdfs-default.xml
		 * 然后再加载classpath下的hdfs-site.xml
		 */
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "bigdata");
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		conf.addResource("config/mapred-site.xml");
		conf.addResource("config/yarn-site.xml");

		// conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		// 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
		conf.set("dfs.replication", "2");
		conf.set("dfs.block.size", "64m");

		// 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
		// 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
		fs = FileSystem.get(conf);
	}

	@Before
	public void init() throws Exception {
//		initLocalFS();
//		initHDFS();
		initHDFSWithConf();
	}

	/**
	 * 创建文件夹
	 */
	@Test
	public void testMkdir() throws Exception {
		System.out.println(fs.mkdirs(new Path("/ccc/bbb/aaa")));
	}

	/**
	 * 上传文件
	 */
	@Test
	public void testCopyFromLocal() throws Exception {
		// src : 要上传的文件所在的本地路径
		// dst : 要上传到hdfs的目标路径
		Path src = new Path("C:/software/hadoop-eclipse-plugin-2.7.7.jar");
		Path dst = new Path("/");
		fs.copyFromLocalFile(src, dst);
	}

	/**
	 * 下载文件
	 */
	@Test
	public void testCopyToLocal() throws Exception {
		fs.copyToLocalFile(new Path("/wordcount/input/helloWorld.txt"), new Path("c:/"));
//		fs.copyToLocalFile(new Path("/wordcount/input/helloWorld.txt"), new Path("d:/"), true);
	}

	/**
	 * 删除文件 或者 文件夹
	 */
	@Test
	public void testRemoveFileOrDir() throws Exception {
		// 删除文件或者文件夹，如果文件夹不为空，这第二个参数必须有， 而且要为true
		fs.delete(new Path("/ccc/bbb"), true);
	}

	/**
	 * 重命名文件 或者 文件夹
	 */
	@Test
	public void testRenameFileOrDir() throws Exception {
		// fs.rename(new Path("/ccc"), new Path("/vvv"));
		fs.rename(new Path("/hadoop-eclipse-plugin-2.6.4.jar"), new Path("/eclipsePlugin.jar"));
	}

	/**
	 * 显示 指定文件夹下  所有的文件
	 */
	@Test
	public void testListFiles() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/wordcount"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.print(fileStatus.getPath() + "\t");
			System.out.print(fileStatus.getPath().getName() + "\t");
			System.out.print(fileStatus.getBlockSize() + "\t");
			System.out.print(fileStatus.getPermission() + "\t");
			System.out.print(fileStatus.getReplication() + "\t");
			System.out.println(fileStatus.getLen());

			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation bl : blockLocations) {
				System.out.println("Block Length:" + bl.getLength() + "   Block OffSet:" + bl.getOffset());
				String[] hosts = bl.getHosts();
				for (String str : hosts) {
					System.out.print(str + "\t");
				}
				System.out.println();
			}

			System.out.println("---------------------------------------");
		}
	}

	/**
	 * 查看指定文件下 的 文件 或者 文件夹。 不包含子文件夹下的内容
	 */
	@Test
	public void testListStatus() throws Exception {
		FileStatus[] listStatus = fs.listStatus(new Path("/wordcount"));
		String flag = "";
		for (FileStatus status : listStatus) {
			if (status.isDirectory()) {
				flag = "Directory";
			} else {
				flag = "File";
			}
			System.out.println(flag + "\t" + status.getPath());
		}
	}
	
	/**
	 * 关闭 FS 示例对象
	 */
	@After
	public void close(){
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
