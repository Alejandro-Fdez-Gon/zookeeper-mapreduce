package es.upm.dit.cnvr_fcon.ZK;

import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper; 
import org.apache.zookeeper.data.Stat;


public class CreateSession implements Watcher{
	private static final int SESSION_TIMEOUT = 5000;
	private String[] hosts;
	private ZooKeeper zk;
	private boolean isConnected = false;
	public Lock lock = new ReentrantLock();

	public CreateSession() {}

	public ZooKeeper ConnectSession(String[] hosts) {
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session
		try {

			try {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, this);
				//Stat s = zk.exists("/",false);
			} 
			catch (Exception e) {
				System.err.println(e);
				return null;
			}

		} catch (Exception e) {
			System.err.println(e);
			return null;
		}
		return zk;

	}

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Created session");
			System.out.println(event.toString());
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}


	public static void main(String[] args) {

		// This is static. A list of zookeeper can be provided for decide where to connect
		String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

		CreateSession cs = new CreateSession();
		ZooKeeper zk = cs.ConnectSession(hosts);
		if (zk == null) {
			System.out.println("No se ha creado la conexión");
		} else {
			System.out.println("Se ha creado la conexión");
		}
	}


}
