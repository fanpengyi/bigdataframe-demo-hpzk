package com.homework.zk;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/25.
 */
public class MyOwnConstant {

    public static String CONNECT_ZK = "10.19.2.54:2181,10.19.2.55:2181,10.19.2.56:2181";


    public static int  TIME_OUT = 3600000;


    public static String LOCK_PARENT_NODE="/parent_locks";

    public static String LOCK_SUB_NODE = "/sub_locks";


    public static String HA_PARENT_NODE="/cluster_ha";

    public static String HA_ACTIVE = "/active";


    public static String HA_STANDBY_NODE="/standby";


    public static String HA_LOCK="/lock";



    public static String CONFIG_PARENT_NODE = "/config";

    public static String UPDOWN_PARENT_NODE = "/servers_updown";
    public static String UPDOWN_CHILD_NODE = UPDOWN_PARENT_NODE+"/child_updown";




}
