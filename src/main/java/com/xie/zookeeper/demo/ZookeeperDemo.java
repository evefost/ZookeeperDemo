package com.xie.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xieyang on 17/1/19.
 */
public  class ZookeeperDemo {

    public static  void   main(String[] args){
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper("172.16.165.128:2181", 3000, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("事件触发:"+watchedEvent.getType());
                }
            });
           // createDemo(zk);
            //update(zk);
            //delete(zk);
            //aclDemo(zk);
            watcherDemo(zk);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            if(zk !=null){
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建节点
     * @param zk
     * @throws KeeperException
     * @throws InterruptedIOException
     * @throws InterruptedException
     */
    private static void  createDemo(ZooKeeper zk) throws KeeperException, InterruptedIOException, InterruptedException {
       if(zk.exists("/node_2",true) == null){
           //节点一定以'/',开头
           zk.create("/node_2","abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

       }else {
           System.out.println("/node_2 已存...");
       }
        System.out.println(new String(zk.getData("/node_2",true,null)));
    }

    /**
     * 更新节点
     * @param zk
     * @throws KeeperException
     * @throws InterruptedIOException
     * @throws InterruptedException
     */
    private static void  update(ZooKeeper zk) throws KeeperException, InterruptedIOException, InterruptedException {
        //
        zk.setData("/node_2","www".getBytes(), -1);//-1 表示不需要判断版本,作更新
        System.out.println(new String(zk.getData("/node_2",true,null)));
    }

    /**
     * 删除节点
     * @param zk
     * @throws KeeperException
     * @throws InterruptedIOException
     * @throws InterruptedException
     */
    private static void  delete(ZooKeeper zk) throws KeeperException, InterruptedIOException, InterruptedException {
        zk.delete("/node_2", -1);//-1 表示不需要判断版本,作更新
        System.out.println(new String(zk.getData("/node_2",true,null)));
    }

    /**
     * 权限控制
     * 自定义权限
     *  digest 模式
     *  world
     *  auth
     *  ip
     * @param zk
     * @throws KeeperException
     * @throws InterruptedIOException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    private static void  aclDemo(ZooKeeper zk) throws KeeperException, InterruptedIOException, InterruptedException, NoSuchAlgorithmException {
        if(zk.exists("/node_3",true) == null){
            ACL acl = new ACL(ZooDefs.Perms.ALL,new Id("digest", DigestAuthenticationProvider.generateDigest("root:root")));
            List<ACL> acls= new ArrayList<ACL>();
            acls.add(acl);
            zk.create("/node_3","node333333".getBytes(), acls ,CreateMode.PERSISTENT);
            zk.addAuthInfo("digest","root:root".getBytes());
            System.out.println(new String(zk.getData("/node_3",true,null)));
        }
    }

    private static void  watcherDemo(ZooKeeper zk) throws KeeperException, InterruptedIOException, InterruptedException {
        if(zk.exists("/node_4",true) == null){
            //节点一定以'/',开头
            zk.create("/node_4","abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        }else {
            System.out.println("/node_4 已存...");
        }
        byte[] rsByt = zk.getData("/node_4", new Watcher() {

            public void process(WatchedEvent watchedEvent) {
                System.out.println("事件触发:"+watchedEvent.getType());
            }
        },null);
        System.out.println(new String(rsByt));
        zk.setData("/node_4","pk".getBytes(),-1);
    }

}
