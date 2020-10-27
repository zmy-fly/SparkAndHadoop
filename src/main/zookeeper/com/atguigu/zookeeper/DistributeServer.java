package com.atguigu.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author Bonze
 * @create 2020-10-19-0:33
 */
public class DistributeServer {


	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

		DistributeServer server = new DistributeServer();
		//1.连接zookeeper集群
		server.getConnect();

		//2.注册节点
		server.regist(args[0]);

		//3.业务逻辑处理
		server.business();


	}
	private void business() throws InterruptedException {
		Thread.sleep(Long.MAX_VALUE);
	}


	private void regist(String hostname) throws KeeperException, InterruptedException {
		String s = zooKeeper.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname + "is online");

	}




	private String connectString = "master2:2181,slave1:2181,slave2:2181";
	private int sessionTime = 2000;
	private ZooKeeper zooKeeper;

	private void getConnect() throws IOException {
		zooKeeper = new ZooKeeper(connectString, sessionTime, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {

			}
		});
	}

}
