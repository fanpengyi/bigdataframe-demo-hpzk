package com.homework.zk.template.配置管理;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * <p>
 * 描述：
 * <p>
 * 作用： 模拟一个真正用来对配置信息进行 添加 修改 删除的操作的 客户端
 */
public class Client {

    private static final String Connect_String = "bigdata02:2181,bigdata03:2181,bigdata04:2181,bigdata05:2181";

    private static final int Session_Timeout = 4000;

    private static final String PARENT_NODE = "/config";

    private static final String key = "name";
    private static final String value = "huangbo";
    private static final String value_new = "huangbo_copy";

    public static void main(String[] args) throws Exception {

        /**
         * 第一步：  获取 zookeeper 连接
         */
        ZooKeeper zk = new ZooKeeper(Connect_String, Session_Timeout, null);

        /**
         * 第二步： 先判断 /config 节点存在与否
         */
        Stat exists = zk.exists(PARENT_NODE, null);
        if (exists == null) {
            zk.create(PARENT_NODE, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        /**
         * 第三部： 模拟实现 添加 修改 删除
         */
        String path = PARENT_NODE + "/" + key;

        // 第三部： 增加一项配置
        zk.create(path, value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // 第三部： 修改一项配置
//		zk.setData(path, value_new.getBytes(), -1);

        // 第三部： 删除一项配置
//		zk.delete(path, -1);

        zk.close();
    }
}
