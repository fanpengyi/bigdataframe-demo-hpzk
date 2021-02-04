package com.homework.zk.myhomework.configmanager;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/26.
 */
public class ConfigClient {

    private static final String CONNECT_ZK = MyOwnConstant.CONNECT_ZK;

    private static final int TIME_OUT = MyOwnConstant.TIME_OUT;

    private static final String PARENT_NODE = MyOwnConstant.CONFIG_PARENT_NODE;


    private static final String key1 = "name";
    private static final String key2 = "age";
    private static final String key3 = "sex";
    private static final String key4 = "sex23";
    private static final String key5 = "sex56";

    private static final String value = "zs";

    private static final String value_new = "ls2";



    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        ZooKeeper zk = new ZooKeeper(CONNECT_ZK, TIME_OUT, null);


        //1 创建 父节点

        Stat stat = zk.exists(PARENT_NODE, false);

        if(stat == null){
            zk.create(PARENT_NODE,PARENT_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String path = PARENT_NODE+"/"+key5;
        //2 创建配置

        //zk.create(path,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);


        //3 删除配置

        zk.delete(path,-1);


        //4 修改配置

        //zk.setData(path,value_new.getBytes(), -1);


    }



}
