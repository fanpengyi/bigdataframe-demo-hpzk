package com.homework.zk.myhomework;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/25.
 * <p>
 * zk 实现分布式锁
 */
public class MyOwnDistributeLock {


    private static String CONNECT_ZK = MyOwnConstant.CONNECT_ZK;


    private static int TIME_OUT = MyOwnConstant.TIME_OUT;

    private static String PARENT_NODE = MyOwnConstant.LOCK_PARENT_NODE;

    private static String SUB_NODE = MyOwnConstant.LOCK_SUB_NODE;


    private static ZooKeeper zk;


    private static String connectPath = "";


    public static void main(String[] args) throws Exception {


        MyOwnDistributeLock myOwnDistributeLock = new MyOwnDistributeLock();

        myOwnDistributeLock.getZK();

        Stat stat = zk.exists(PARENT_NODE, false);

        if(stat == null){
            zk.create(PARENT_NODE,PARENT_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }

        // 注册子节点监听
        zk.getChildren(PARENT_NODE,true);

        // 创建临时子节点

        connectPath = zk.create(PARENT_NODE+SUB_NODE,SUB_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);


        Thread.sleep(Long.MAX_VALUE);

        zk.close();
    }


    public void getZK() throws Exception {

        zk = new ZooKeeper(CONNECT_ZK, TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                Event.EventType type = event.getType();
                String path = event.getPath();

                if(Event.EventType.NodeChildrenChanged.equals(type) && PARENT_NODE.equals(path)){


                    try {
                        List<String> children = zk.getChildren(PARENT_NODE, true);

                        //最小节点的抢到做锁进行业务处理

                        Collections.sort(children);

                        if((PARENT_NODE+"/"+children.get(0)).equals(connectPath)){

                            handleBusiness2(connectPath);

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


    private void handleBusiness2(String path) throws Exception {

        System.out.println(path+" is work...");

        //模拟业务运行

        Thread.sleep(new Random().nextInt(4000));

        zk.delete(path,-1);

        System.out.println(path+"is done...");

        //模拟重新抢占线程，处理过任务的线程 不再抢占资源 ，delete 的时候会出发回调函数
        Thread.sleep(new Random().nextInt(4000));

        connectPath = zk.create(PARENT_NODE+SUB_NODE,SUB_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);



    }


}
