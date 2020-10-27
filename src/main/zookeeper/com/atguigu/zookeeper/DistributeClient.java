package com.atguigu.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Bonze
 * @create 2020-10-19-0:48
 */
public class DistributeClient {

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		//获取集群连接
		getConnect();
		//注册监听
		getChildren();
		//业务逻辑
		business();

	}
	private static void business() throws InterruptedException {
		Thread.sleep(Long.MAX_VALUE);
	}

	private static void getChildren() throws KeeperException, InterruptedException {
		List<String> children = zooKeeper.getChildren("/servers", true);
		//存储服务器节点
		ArrayList<String> hosts = new ArrayList<>();
		children.forEach(k -> {
			try {
				byte[] data =zooKeeper.getData("/servers/" + k,true,null);
				hosts.add(new String(data));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		System.out.println(hosts);
	}


	private static String connectString = "master2:2181,slave1:2181,slave2:2181";
	private static int sessionTime = 2000;
	private static ZooKeeper zooKeeper;

	private static void getConnect() throws IOException {
		zooKeeper = new ZooKeeper(connectString, sessionTime, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				try {
					getChildren();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

}
