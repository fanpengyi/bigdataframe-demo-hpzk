package com.homework.zk.template.配置管理;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513 日期： 2017年12月19日 下午4:10:39
 * 
 * 描述：
 * 
 * 一个server实例就代表了一个 外部集群的 一个 服务器节点
 * 
 * 就是用来监听配置信息的改变（到底是改变了什么）
 * 
 * server程序其实就是模拟了一个服务器节点的监听程序
 */
public class DataNode {

	//private static final String Connect_String = "bigdata02:2181,bigdata03:2181,bigdata04:2181,bigdata05:2181";
	private static final String Connect_String = MyOwnConstant.CONNECT_ZK;

	private static final int Session_Timeout = 400000;

	private static final String PARENT_NODE = "/config";

	static List<String> oldChildNodeList = null;
	static ZooKeeper zk = null;

	public static void main(String[] args) throws Exception {

		/**
		 * 第一步： 获取zookeeper连接
		 */
		zk = new ZooKeeper(Connect_String, Session_Timeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {

				EventType type = event.getType();
				String path = event.getPath();
				KeeperState state = event.getState();

				/**
				 * path : /config + /config/name
				 * type : NodeChildrenChanged NodeDataChanged NodeDeleted
				 */

				/**
				 * 当前这个判断的作用就是用来屏蔽获取连接时的那个触发
				 * type；  None
				 * path： null
				 */
				if (state.getIntValue() == 3 && path != null) {

					// 触发了监听之后的 子节点的列表信息
					List<String> newChildNodeList = null;
					try {
						newChildNodeList = zk.getChildren(PARENT_NODE, true);
					} catch (KeeperException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}

					/**
					 * 虽然此处响应的逻辑有可能是 添加 或者删除， 但是经过判断之后，如果是删除，不做任何处理
					 * 如果添加，那么做对应的业务处理
					 */
					if (path.equals(PARENT_NODE) && type == EventType.NodeChildrenChanged) {

						// 仅仅只是处理添加的逻辑
						if(oldChildNodeList.size() < newChildNodeList.size()){
							String diffServer = getDiffBetweenTwoList(oldChildNodeList, newChildNodeList);
							byte[] data = null;
							try {
								data = zk.getData(PARENT_NODE + "/" + diffServer, true, null);
							} catch (KeeperException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							System.out.println("服务器集群  add 了一项配置： key="+diffServer+", value="+new String(data));
						}
						

						/**
						 * 父节点下的一个子节点的删除 （删除）
						 * 删除子节点 ===== 删除一项配置
						 */
					} else {

						for (String child : oldChildNodeList) {

							/**
							 * childPath到底应该怎么获取？ 获取触发之前 process方法执行之前。。 也就是
							 * 客户端的 删除 和 修改的操作之前的
							 */
							String childPath = PARENT_NODE + "/" + child;

							if (path.equals(childPath) && type == EventType.NodeDeleted) {

								// 删除的配置的key
								String diffServer = getDiffBetweenTwoList(newChildNodeList, oldChildNodeList);
								System.out.println("服务器集群  delete 了一项配置： key="+diffServer);
								
							}
						}

					}
					
					/**
					 * 父节点下的一个子节点的数据变化事件 （修改）
					 */
					for (String child : oldChildNodeList) {

						String childPath = PARENT_NODE + "/" + child;
						if (path.equals(childPath) && type == EventType.NodeDataChanged) {

							byte[] data= null;
							try {
								data = zk.getData(childPath, true, null);
							} catch (KeeperException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							String newValue = new String(data);
							System.out.println("服务器集群  update 了一项配置： key="+child + ", newValue="+ newValue);
							
							break;
						}
					}
					
					
					/**
					 * 新老子节点列表的迭代
					 * 已经响应了的事件，统统都得再次加上
					 * 特别是新增加的那个子节点的数据变化事件，必须得新添加上
					 */
					oldChildNodeList = newChildNodeList;
				}
			}
		});

		/**
		 * 第二步： 先判断 /config 节点存在与否
		 */
		Stat exists = zk.exists(PARENT_NODE, null);
		if (exists == null) {
			zk.create(PARENT_NODE, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		/**
		 * 第三步：添加监听
		 * 监听有两种：

		 * 1、/config节点的 NodeChildrenChanged  -- 新增或者删除配置
		 * 2、/config节点下的子节点 的NodeDataChanged  - 修改配置
		 */
		oldChildNodeList = zk.getChildren(PARENT_NODE, true);

		for (String child : oldChildNodeList) {
			String childPath = PARENT_NODE + "/" + child;

			// 添加子节点的 NodeDataChanged
			zk.getData(childPath, true, null);
		}

		/**
		 * 第四步： 让程序一致运行
		 * 当前的代码的执行分为两个线程运行
		 * 主线程 + 监听线程 （根据event的数据来判断掉地调用哪个watcher中的process方法运行）
		 * 最后的那些打印值都是 process方法中打印出来， 而且process方法的调用运行不在主线程中
		 */
		Thread.sleep(Long.MAX_VALUE);
	}

	public static String getDiffBetweenTwoList(List<String> smallList, List<String> bigList) {

		// 默认实现： 增加了一个节点
		List<String> big = bigList;
		List<String> small = smallList;

		for (String val : big) {
			if (!small.contains(val)) {
				return val;
			}
		}

		return null;
	}
}
