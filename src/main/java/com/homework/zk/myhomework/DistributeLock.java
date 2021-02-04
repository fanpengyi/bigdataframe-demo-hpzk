package com.homework.zk.myhomework;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/22.
 *
 *
 * 需求： 多个客户端 需要同时访问同一个资源 但同时只允许一个客户端进行访问
 *
 * 思路：多个客户端都去父 znode 下 写入一个 子 znode，能写入成功的去执行访问 写入不成功的 就等待
 *
 * 场景：使用 二维码 支付，同时只能有一个成功
 *
 *
 */
public class DistributeLock {

    private static final String ZK_CONNECT = "10.19.2.54:2181,10.19.2.55:2181,10.19.2.56:2181";

    private static final int sessionTimeout = 400000;

    private static final String PARENT_NODE="/parent_locks";

    private static final String SUB_NODE = "/sub_client";

    static ZooKeeper zk = null;


    private static  String currentPath = "";



    public static void main(String[] args) throws Exception {

        DistributeLock distributeLock = new DistributeLock();
        //1 获取 ZK 连接

        distributeLock.getZookeeperConnect();

        //2 查看父节点是否存在，不存在则创建一个

        Stat stat = zk.exists(PARENT_NODE, false);

        if(stat == null){
            zk.create(PARENT_NODE,PARENT_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }


        //3 监听父节点 getExsit  getData getChildren

        zk.getChildren(PARENT_NODE,true);

        //4 往父节点下注册节点，注册临时节点，好处就是当宕机或者断开连接时 该节点自动删除

        // 一个客户端 注册一个节点 触发 回调 函数,模拟 每个客户端进行业务操作

        currentPath = zk.create(PARENT_NODE+SUB_NODE,SUB_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);

        Thread.sleep(Long.MAX_VALUE);

        zk.close();




    }

    /**
     * 拿到 zookeeper 集群的连接
     *
     */
    public void getZookeeperConnect() throws Exception {

        zk  = new ZooKeeper(ZK_CONNECT, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                //匹配看是不是子节点变化，并且监听路径也正确

                if(event.getType() == Event.EventType.NodeChildrenChanged && PARENT_NODE.equals(event.getPath())){


                    try {
                        //获取父节点的所有子节点 设置循环监听
                        List<String> children = zk.getChildren(PARENT_NODE, true);

                        Collections.sort(children);

                        //序号最小的 进行业务 操作
                        if((PARENT_NODE+"/"+children.get(0)).equals(currentPath)){
                            //进行业务处理
                            handleBusiness(currentPath);
                        }else{

                            System.out.println(" not me");
                        }



                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                }



            }
        });



    }



    public void handleBusiness(String create) throws InterruptedException, Exception {

        System.out.println(create + " is working");

        //模拟业务代码执行 0-4 秒
        Thread.sleep(new Random().nextInt(4000));

        //模拟业务处理完成

        zk.delete(currentPath,-1);

        System.out.println(create+" is work done....");

        //以下是业务处理完成 后续进行程序抢注 预留一段时间 模拟服务器抢注该子节点的顺序

        Thread.sleep(new Random().nextInt(4000));

        currentPath = zk.create(PARENT_NODE+SUB_NODE,SUB_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);


    }











}
