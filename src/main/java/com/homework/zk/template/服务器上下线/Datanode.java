package com.homework.zk.template.服务器上下线;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 用来 模拟服务器的上线和下线 怎么模拟？
 * <p>
 * 假如说有在zookeeper的文件系统当中有一个znode叫 /servers
 * 那么我们就认为这个/servers节点下面的一个子znode就是一台datanode
 */
public class Datanode {

    private static int index = 104;

    private static ZooKeeper zk = null;

    public static void main(String[] args) throws Exception {

    	zk = new ZooKeeper(Constant.ConnectStr, Constant.TimeOut, null);

        // 第四步：上线服务器
//		upServer();

        // 第四步：测试下线服务器
		downServer();

        zk.close();
    }

    public static void upServer() throws KeeperException, InterruptedException {
		String str = zk.create(Constant.ChildNode + index, "服务器".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(str + "上线成功");
	}

	public static void downServer() throws KeeperException, InterruptedException {
		String path = Constant.ChildNode+ index;
		zk.delete(path, -1);
		if(zk.exists(path, null) == null){
			System.out.println(path + " 这台服务器下线");
		}else{
			System.out.println("delete failure");
		}
	}

}
