package com.homework.zk.myhomework.updown;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/26.
 */
public class MyNameNode {



    private static final String CONNECT_ZK = MyOwnConstant.CONNECT_ZK;

    private static final int TIME_OUT = MyOwnConstant.TIME_OUT;

    private static final String PARENT = MyOwnConstant.UPDOWN_PARENT_NODE;

    private static final String CHILD  = MyOwnConstant.UPDOWN_CHILD_NODE;


    private static ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {


        zk = new ZooKeeper(CONNECT_ZK, TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {


                String path = event.getPath();
                Event.EventType type = event.getType();

                if(Event.EventType.NodeChildrenChanged.equals(type) && path.equals(PARENT)){

                    //子节点变化
                    try {

                        List<String> children = zk.getChildren(path, true);

                        System.out.println("node list:  "+children);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                }else if(Event.EventType.NodeDataChanged.equals(type) && path.equals(PARENT)){
                    // 值变化
                    System.out.println("namenode changed value");
                    try {
                        byte[] data = zk.getData(path, true, null);

                        System.out.println("value :"+new String(data));


                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                }




            }
        });


        // 父节点是否存在
        Stat stat = zk.exists(PARENT, null);

        if(stat == null){
            zk.create(PARENT,PARENT.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }


        // 节点注册监听
        // 子节点改变 和 数值改变？

        zk.getChildren(PARENT,true);

        zk.getData(PARENT,true,null);


        //等待执行
        Thread.sleep(Long.MAX_VALUE);








    }







}
