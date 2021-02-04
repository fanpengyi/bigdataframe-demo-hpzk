package com.homework.zk.template;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 需求：多个客户端，需要同时访问同一个资源，但同时只允许一个客户端进行访问。
 * 设计思路：多个客户端都去父 znode 下写入一个子 znode ，能写入成功的去执行访问，写入不成功的等待
 *
 *  比如：
 *  	支付
 *  	   二维码，只能有一个人支付成功
 *
 *  	   同时只能有一个人
 */
public class MyDistributeLock {

	private static final String connectStr = "bigdata02:2181,bigdata03:2181,bigdata04:2181";
	private static final int sessionTimeout = 4000;
	private static final String PARENT_NODE = "/parent_locks";
	private static final String SUB_NODE = "/sub_client";
	static ZooKeeper zk = null;

	private static String currentPath = "";

	public static void main(String[] args) throws Exception {

		MyDistributeLock mdc = new MyDistributeLock();

		// 1、拿到zookeeper链接
		mdc.getZookeeperConnect();

		// 2、查看父节点是否存在，不存在则创建
		Stat exists = zk.exists(PARENT_NODE, false);
		if (exists == null) {
			zk.create(PARENT_NODE, PARENT_NODE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		// 3、监听父节点
		zk.getChildren(PARENT_NODE, true);

		// 4、往父节点下注册节点，注册临时节点，好处就是，当宕机或者断开链接时该节点自动删除
		currentPath = zk.create(PARENT_NODE + SUB_NODE, SUB_NODE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		Thread.sleep(Long.MAX_VALUE);
		// 5、关闭zk链接
		zk.close();
	}

	/**
	 * 拿到zookeeper集群的链接监听
	 */
	public void getZookeeperConnect() throws Exception {
		zk = new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 匹配看是不是子节点变化，并且监听的路径也要对
				if (event.getType() == EventType.NodeChildrenChanged && event.getPath().equals(PARENT_NODE)) {
					try {
						// 获取父节点的所有子节点, 并继续监听
						List<String> childrenNodes = zk.getChildren(PARENT_NODE, true);
						// 匹配当前创建的znode是不是最小的znode
						Collections.sort(childrenNodes);
						if ((PARENT_NODE + "/" + childrenNodes.get(0)).equals(currentPath)) {
							// 处理业务
							handleBusiness(currentPath);
						}else{
							System.out.println("not me");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	public void handleBusiness(String create) throws Exception {
		System.out.println(create + " is working......");
		// 线程睡眠0-4秒钟，是模拟业务代码处理所消耗的时间
		Thread.sleep(new Random().nextInt(4000));
		// 模拟业务处理完成
		zk.delete(currentPath, -1);
		System.out.println(create + " is done ......");
		// 线程睡眠0-4秒， 是为了模拟客户端每次处理完了之后再次处理业务的一个时间间隔，最终的目的就是用来打乱你运行的多台
		// 服务器抢注该子节点的顺序
		Thread.sleep(new Random().nextInt(4000));
		// 模拟去抢资源锁
		currentPath = zk.create(PARENT_NODE + SUB_NODE, SUB_NODE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
}
