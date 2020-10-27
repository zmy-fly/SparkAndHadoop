package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import javax.xml.bind.SchemaOutputResolver;
import java.io.IOException;
import java.util.List;

/**
 * @author Bonze
 * @create 2020-10-18-23:30
 */
public class ZookeeperHello {
	private String connectString = "master2:2181,slave1:2181,slave2:2181";
	private int sessionTimeOut = 2000;
	private ZooKeeper zkClient;

	@Before
	public void init() throws IOException {

		zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				System.out.println("---------start---------");
				List<String> children = null;
				try {
					children = zkClient.getChildren("/", false);
					children.forEach(System.out::println);
					System.out.println("---------end---------");
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	@Test
	public void createNode() throws KeeperException, InterruptedException {
		String path = zkClient.create("/atguigu", "zmynb".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(path);

	}
	// 2.获取子节点 并监控数据的变化
	@Test
	public void getDataAndWatch() throws KeeperException, InterruptedException {
		 // 拿去某一个路径(path)下的所有子节点，并确定是否监听（boolean watch）

		List<String> children = zkClient.getChildren("/", true);
		children.forEach(System.out::println);
		Thread.sleep(Long.MAX_VALUE);
	}

	@Test
	public void exist() throws KeeperException, InterruptedException {
		Stat exists = zkClient.exists("/atguigu", false);
		System.out.println(exists == null? "doesn't exist" : "exists");
	}

}
