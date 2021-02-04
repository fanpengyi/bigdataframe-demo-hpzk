package com.homework.zk.template.服务器上下线;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.List;

/**
 * 作用：用来感知上线 或者 下线的服务器 怎么模拟？
 * namenode 去监控 /servers 节点下面的子节点个数
 * 如果个数增加，表示上线一个datanode
 * 如果个数减少，表示下线一个datanode
 */
public class Namenode {

	static ZooKeeper zk = null;

	public static void main(String[] args) throws Exception {

		// 第一步：拿zookeeper连接
		zk = new ZooKeeper(Constant.ConnectStr, Constant.TimeOut, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 直接打印event的时候，第一次打印的结果是：Type:Node, Null
				String outPath = event.getPath();
				EventType type = event.getType();
				System.out.println(outPath + "  -- " + type.toString());

				if (type == EventType.NodeChildrenChanged && event.getPath().equals(Constant.ParentNode)) {
					List<String> datanodeList;
					try {
						datanodeList = getDatanodeList(zk);
						System.out.println(datanodeList);

						// 做到连续监听
						zk.getChildren(Constant.ParentNode, true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (type == EventType.NodeDataChanged && event.getPath().equals(Constant.ParentNode)) {
					System.out.println("节点数据发生变化");

					try {
						byte[] data = zk.getData(Constant.ParentNode, null, null);
						System.out.println(new String(data));

						zk.getData(Constant.ParentNode, true, null);
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});

		// 第二步：检查/servers节点存在与否，如果不存在，则创建
		if (zk.exists(Constant.ParentNode, null) == null) {
			zk.create(Constant.ParentNode, "huangbo".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			System.out.println(Constant.ParentNode + " 已存在");
		}

		// 第三步：给/servers节点加监听
		zk.getChildren(Constant.ParentNode, true);
		zk.getData(Constant.ParentNode, true, null);

		Thread.sleep(Integer.MAX_VALUE);

		// 关闭zk连接
		zk.close();
	}

	public static List<String> getDatanodeList(ZooKeeper zk) throws Exception {
		// 获取父节点下面的子节点列表。也就是datanode列表
		List<String> children = zk.getChildren(Constant.ParentNode, null);
		return children;
	}
}
