package com.homework.zk.myhomework.configmanager;

import com.homework.zk.MyOwnConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/26.
 * <p>
 * <p>
 * 模拟 server 端启动额外线程 监听机制
 * 进行 配置新增 删除 修改 操作
 */
public class Server {

    private static final String CONNECT_ZK = MyOwnConstant.CONNECT_ZK;

    private static final int TIME_OUT = MyOwnConstant.TIME_OUT;


    private static final String PARENT_NODE = MyOwnConstant.CONFIG_PARENT_NODE;

    private static ZooKeeper zk;

    private static List<String> oldChildNodeList;


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        // 1 过去 zk 连接

        zk = new ZooKeeper(CONNECT_ZK, TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                String path = event.getPath();
                Event.EventType type = event.getType();
                Event.KeeperState state = event.getState();
                //  3 是zk的状态码 的意思是 连接成功
                // 0  断开连接
                //4 认证失败

                /**
                 * 当前这个判断的作用就是用来屏蔽获取连接时的那个触发
                 * type；  None
                 * path： null
                 */
                if (state.getIntValue() == 3 && path != null) {

                    //出发监听机制后 子节点的列表
                    List<String> newChildrenNodes = null;
                    try {
                        newChildrenNodes = zk.getChildren(PARENT_NODE, true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // 新增  删除  和 修改

                    if (Event.EventType.NodeChildrenChanged.equals(type) && path.equals(PARENT_NODE)) {
                        //新增 只处理新增的逻辑 ，看看删除是否会走？？

                        if (oldChildNodeList.size() < newChildrenNodes.size()) {

                            String diffConfigNode = getDiffBetweenLists(oldChildNodeList, newChildrenNodes);

                            byte[] value = null;
                            try {
                                //新添加的配置需要 注册监听
                                value = zk.getData(PARENT_NODE+"/"+diffConfigNode,true,null);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            System.out.println(" add a new config! key = "+diffConfigNode+" value = "+new String(value));

                        }

                    } else  {

                        for (String child : oldChildNodeList) {


                            String childPath = PARENT_NODE+"/"+child;

                            if(path.equals(childPath) && Event.EventType.NodeDeleted.equals(type)){

                                String diffBetweenLists = getDiffBetweenLists(newChildrenNodes, oldChildNodeList);

                                System.out.println("delete one config！ key = "+diffBetweenLists);


                            }

                        }


                    }


                    //修改配置
                    for (String child : oldChildNodeList) {

                        String childPath = PARENT_NODE + "/" + child;
                        if(Event.EventType.NodeDataChanged.equals(type) && path.equals(childPath)){


                            byte[] value = null;

                            try {
                                //修改的新值注册监听
                                value = zk.getData(childPath,true,null);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            System.out.println("update one config!, key = "+child +"value = "+new String(value));
                            break;
                        }

                    }

                    //列表更新
                    //新增加点 与 修改节点 的监控信息必须要添加上

                    oldChildNodeList = newChildrenNodes;

                }


            }
        });


        //2 检验 父节点 是否存在


        Stat stat = zk.exists(PARENT_NODE, false);

        if (stat == null) {
            zk.create(PARENT_NODE, PARENT_NODE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }


        //3 节点监听 两方面  父节点 -- 新增 删除
        // 每个子节点的监听   子节点--- 修改

        oldChildNodeList = zk.getChildren(PARENT_NODE, true);

        for (String subNode : oldChildNodeList) {

            zk.getData(PARENT_NODE + "/" + subNode, true, null);

        }


        //4 等待执行
        Thread.sleep(Long.MAX_VALUE);


    }

    private static String getDiffBetweenLists(List<String> oldChildNodeList, List<String> newChildrenNodes) {


        for (String node : newChildrenNodes) {

            if (!oldChildNodeList.contains(node)) {
                return node;
            }


        }

        return null;


    }


}
