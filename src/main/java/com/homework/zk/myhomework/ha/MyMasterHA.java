package com.homework.zk.myhomework.ha;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/25.
 *
 * 核心业务
 * 如果 A 是第一个上线的 master active
 * 如果 B 是第二个上线的 master 自动 standy
 * 如果 C 3 --- standy
 * A 宕机  B 和 C 竞选自动成为 active
 * 然后 A 上线，自动成为 standy
 *
 * 如果 standy 节点宕机 节点状态不改变
 * active 节点宕机，剩下的 standy 节点竞选 active
 *
 *
 *
 *
 * 思路：
 *
 * 1 先确定 父节点 active 节点 standy 节点 存在 不存在进行创建
 *
 * 2 查看 active 路径下是否有子节点
 *
 *  没有 -- 竞争 active ，先抢占锁 再创建节点 谁创建成功谁就是 active
 *      抢占锁的同时 触发锁回调函数，创建 active 节点
 *      创建 active 路径同时 触发active回调函数 ，判断 active 是有节点的 不做任何操作
 *
 *
 *   有 --  生成 standby 状态，注册监听 active 路径 ，等待 active 宕机 进行active 切换
 *
 *          active 宕机 触发active回调函数，进行active 竞争，先抢占锁
 *              抢到锁的 触发 锁回调函数 进行 active 路径创建
 *
 *
 *
 */


public class MyMasterHA {

    private static final String CONNECT_ZK = MyOwnConstant.CONNECT_ZK;

    private static final int TIME_OUT = MyOwnConstant.TIME_OUT;

    private static String PARENT = MyOwnConstant.HA_PARENT_NODE;
    private static String ACTIVE = MyOwnConstant.HA_ACTIVE;
    private static String STANDBY = MyOwnConstant.HA_STANDBY_NODE;
    private static String LOCK = MyOwnConstant.HA_LOCK;

    private static ZooKeeper zk;


    private static String HOST_NAME = "hadoop08";

    private static final String activeMasterPath = ACTIVE+"/"+HOST_NAME;

    private static final String standByMasterPath = STANDBY+"/"+HOST_NAME;

    private static final CreateMode CM_E = CreateMode.EPHEMERAL;
    private static final CreateMode CM_P = CreateMode.PERSISTENT;



    /**
     * 测试顺序：
     * 1、先启动hadoop03, 成为active
     * 2、再启动hadoop04, 成为standby
     * 3、再启动hadoop05, 成为standby
     * 4、再启动hadoop06, 成为standby
     * 5、停止active节点hadoop03, 那么hadoop04, hadoop05, hadoop06会发生竞选，胜者为active, 假如为hadoop4
     *    那么hadoop05, hadoop06 会成为standby
     * 6、再干掉active节点hadoop04, 那么hadoop05, hadoop06会发生竞选，假如hadoop05胜出，那么hadoop06依然是standby
     * 7、然后上线hadoop03, 当然自动成为standby
     * 8、再干掉hadoop05, 那么hadoop03和hadoop06竞选
     * ........
     */
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        zk = new ZooKeeper(CONNECT_ZK, TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                String path = event.getPath();
                Event.EventType type = event.getType();


                if(Event.EventType.NodeChildrenChanged.equals(type) && path .equals(ACTIVE)){

                    if(getChildrenNum(ACTIVE) == 0){

                        //抢占锁
                        try {
                            zk.exists(LOCK,true);
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        createLockNode(LOCK,HOST_NAME,CM_E,"lock");

                    }else{

                        System.out.println("have one active，do nothing...");
                    }

                    //循环监听
                    try {
                        zk.getChildren(ACTIVE,true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                }else if(Event.EventType.NodeCreated == type && LOCK.equals(path)){

                    //获取 hostname 的值 与 主机名对比 一致的 设置为 active

                    String lockValue = null;
                    try {
                        byte[] data = zk.getData(LOCK, false, new Stat());

                        lockValue = new String(data);

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    //创建 active
                    if(HOST_NAME.equals(lockValue)){

                        //我抢到锁了
                         createNode(activeMasterPath,HOST_NAME,CM_E);

                         // 这个主机名对应的 standby 路径 是否存在 ，修改 standby状态
                         if(exsit(standByMasterPath)){
                             System.out.println(HOST_NAME +" 成功切换到 active！");
                            deleteMyZnode(standByMasterPath)     ;

                         }else{

                             System.out.println(HOST_NAME+" 竞选成功 成为 ACTIVE！");
                         }

                    }else{

                        //不是自己 不动
                    }


                }




            }
        });


        //1 判断父节点 ，ACTIVE 节点 ，STANDBY 节点 是否存在

        if(!exsit(PARENT)){
            createNode(PARENT,PARENT,CM_P);
        }
        if(!exsit(ACTIVE)){
            createNode(ACTIVE,ACTIVE,CM_P);
        }
        if(!exsit(STANDBY)){
            createNode(STANDBY,STANDBY,CM_P);
        }



        //2 进行 active 判断

        if(getChildrenNum(ACTIVE) == 0){
            //没有 active 抢占锁！

            //注册监听
            zk.exists(LOCK,true);

            //抢锁  值存的是 主机名  ，谁抢到锁就存谁的主机名
            createNode(LOCK,HOST_NAME,CM_E);//触发 回调函数

        }else{
             //有 active 状态 自动切换成 standby

             //在 standby/主机名 下创建节点
            createNode(standByMasterPath,HOST_NAME,CM_E);
            System.out.println(HOST_NAME + " 发现active存在，所以自动成为standby");
            //注册监听 active 子节点变化

            zk.getChildren(ACTIVE,true);

        }

        Thread.sleep(Long.MAX_VALUE);

    }

    private static void createLockNode(String lock, String hostName, CreateMode cmE) {
    }

    private static void deleteMyZnode(String standByMasterPath) {

        try {
            zk.delete(standByMasterPath,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private static int getChildrenNum(String active) {

        int num = 0;
        try {
            num = zk.getChildren(active, false).size();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return num;

    }

    private static void createNode(String parent, String parent1, CreateMode cmP) {

        try {
            zk.create(parent,parent1.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,cmP);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("创建节点失败 或者 节点已经存在");
        }


    }


    private static void createLockNode(String lock, String hostName, CreateMode cmE,String msg) {

        try {
            zk.create(lock,hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,cmE);
        } catch (Exception e) {
            if(msg.equals("lock")){
                System.out.println("我没有抢到锁，等下一波");
            }
        }
    }







    private static boolean exsit(String parent) {

        try {
            Stat stat = zk.exists(parent, false);
            if(stat == null){
                return false;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }


}
