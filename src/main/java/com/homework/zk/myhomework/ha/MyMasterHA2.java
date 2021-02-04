package com.homework.zk.myhomework.ha;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/26.
 */
public class MyMasterHA2 {


    private final static String  CONNECT_ZK = MyOwnConstant.CONNECT_ZK;
    private final static int  TIME_OUT = MyOwnConstant.TIME_OUT;


    private static final String PARENT = MyOwnConstant.HA_PARENT_NODE;
    private static final String ACTIVE = MyOwnConstant.HA_ACTIVE;
    private static final String STANDBY = MyOwnConstant.HA_STANDBY_NODE;
    private static final String LOCK = MyOwnConstant.HA_LOCK;


    private static String HOST_NAME="hadoop03";

    private static final String activeHaPath = ACTIVE+"/"+HOST_NAME;
    private static final String standbyHaPath = STANDBY+"/"+HOST_NAME;



    private static ZooKeeper zk;




    public static void main(String[] args) throws Exception {


        //
        zk = new ZooKeeper(CONNECT_ZK, TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //关注两个事件 1 Active 子节点变化 2 LOCK 创建
                String path = event.getPath();
                Event.EventType type = event.getType();

                if (Event.EventType.NodeChildrenChanged.equals(type) && ACTIVE.equals(path)) {


                    if (getNewChildren(ACTIVE) == 0) {

                        //抢占锁
                        try {
                            zk.exists(LOCK, true);
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }


                        createLockZnode(LOCK, HOST_NAME, CreateMode.EPHEMERAL, "lock");

                    } else {

                    }

                    //循环监听
                    try {
                        zk.getChildren(ACTIVE, true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                } else if (Event.EventType.NodeCreated.equals(type) && LOCK.equals(path)) {

                    //获取锁的值
                    String lockValue = null;

                    try {
                        byte[] data = zk.getData(path, false, new Stat());
                        lockValue = new String(data);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    //哪一个客户端抢到了锁 才会 切换状态
                    if (HOST_NAME.equals(lockValue)) {
                        createZnode(activeHaPath, HOST_NAME, CreateMode.EPHEMERAL);
                        //如果是standby 切换的 要删除路径
                        if(exsitNew(standbyHaPath)){
                            deleteZnode(standbyHaPath);
                            System.out.println(HOST_NAME + "切换了状态，由 standby 成为 active！");
                        }else{
                            System.out.println(HOST_NAME + "竞选成为了 Active！");
                        }

                    } else {
                        System.out.println( HOST_NAME+"没有抢到锁，不做任何改变！");
                    }


                }


            }
        });


        //2 检查路径

        if (!exsitNew(PARENT)) {
            createZnode(PARENT, PARENT, CreateMode.PERSISTENT);
        }

        if (!exsitNew(ACTIVE)) {
            createZnode(ACTIVE, ACTIVE, CreateMode.PERSISTENT);
        }

        if (!exsitNew(STANDBY)) {
            createZnode(STANDBY, STANDBY, CreateMode.PERSISTENT);
        }

        //3 节点判断

        if (getNewChildren(ACTIVE) == 0) {

            //还没有 active 节点

            //抢占锁
            //先注册监听
            zk.exists(LOCK, true);

            //创建锁

            createZnode(LOCK, HOST_NAME, CreateMode.EPHEMERAL);

        } else {
            // 已经有 active 了 ，转成 standby 吧

            createZnode(standbyHaPath, HOST_NAME, CreateMode.EPHEMERAL);
            System.out.println("active 已经存在了,"+HOST_NAME+"成为了 standby！");

            //注册 Active 的监听事件 随时等待切换

            zk.getChildren(ACTIVE, true);

        }

        // 等待执行
        Thread.sleep(Long.MAX_VALUE);

        zk.close();

    }
    private static void deleteZnode(String standbyHaPath) {
        try {
            zk.delete(standbyHaPath,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private static void createLockZnode(String lock, String hostName, CreateMode nodeType, String msg) {
        try {
            zk.create(lock,hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,nodeType);
        } catch (Exception e) {
            if(msg.equals("lock")){
                System.out.println(hostName+"没有抢到锁！,等待下一次机会~");
            }

        }

    }

    private static int getNewChildren(String active) {

        int num = 0;
        try {
            num = zk.getChildren(active, false).size();
        } catch (KeeperException e) {


        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return num;

    }

    private static void createZnode(String parent, String value, CreateMode nodeType) {

        try {
            zk.create(parent,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,nodeType);
        } catch (Exception e) {
            System.out.println(parent+"已经存在！");
        }


    }

    private static boolean exsitNew(String parent) {

        try {
            Stat exists = zk.exists(parent, false);
            if(exists == null){
                return  false;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return true;

    }


}
