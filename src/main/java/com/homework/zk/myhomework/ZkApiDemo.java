package com.homework.zk.myhomework;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/22.
 */
public class ZkApiDemo {


    //客户端去请求链接的时候服务器地址信息


    /*server.1=10.19.2.54:2888:3888
    server.2=10.19.2.55:2888:3888
    server.3=10.19.2.56:2888:3888*/

    private static String connectString = "10.19.2.54:2181,10.19.2.55:2181,10.19.2.56:2181";

    //客户端去请求链接的超时时长


    private static int sessionTimeout = 10000*100;

    //节点名称 统一命名
    private static String znode = "/zk/testzk";


    public static void main(String[] args) throws Exception {


        // 创建 zk 链接
        ZooKeeper zk = new ZooKeeper(connectString,sessionTimeout,null);

        //1 创建znode

      /*  String createNode = zk.create(znode, "data1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(createNode+"节点创建成功！");

        System.out.println("================================");


        //2 查看节点
        byte[] data = zk.getData(znode, null, null);

        System.out.println("查看节点信息："+new String(data));
        System.out.println("================================");*/

        // 3 修改节点数据
        Stat setData = zk.setData(znode, "data2".getBytes(), -1);

        Date date = new Date(setData.getMtime());
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();

        LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        String format = fmt.format(localDateTime);

        System.out.println("修改节点的时间："+format);

        byte[] data1 = zk.getData(znode, null, null);
        System.out.println("修改节点内容："+new String(data1));

        System.out.println("================================");

        //4 获取 znode 的权限信息
        List<ACL> aclList = zk.getACL(znode, new Stat());

        for (ACL acl : aclList) {
            System.out.println("权限信息："+acl.getPerms());
        }

        System.out.println("================================");

        //5 判断节点是否存在
        Stat exists = zk.exists(znode, null);

        System.out.println(null != exists ? true:false);

        //6 获取子节点
        List<String> children = zk.getChildren("/", null);

        for (String child : children) {
            System.out.println("子节点信息："+child);
        }
        System.out.println("================================");

        //7 删除节点
        zk.delete(znode,-1);
        Stat exists1 = zk.exists(znode, null);

        System.out.println(null == exists1 ? "删除成功":"删除失败");

        zk.close();
    }



}
