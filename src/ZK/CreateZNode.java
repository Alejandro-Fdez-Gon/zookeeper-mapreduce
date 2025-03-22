package es.upm.dit.cnvr_fcon.ZK;

import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class CreateZNode {

	private ZooKeeper  zk; 
	private String     name;
	private byte[]     bytes;
	private CreateMode mode;
	private boolean    isCreated = false;
	private Watcher    watcher   = null;

	public CreateZNode(ZooKeeper zk, String name, byte[] bytes, CreateMode mode) {
		this.zk    = zk;
		this.name  = name;
		this.bytes = bytes;
		this.mode  = mode;
	}

	public String createZNode() {

		String response = null;

		if (this.zk == null) return null;

		if (isCreated) return null;

		try {
			Stat s = zk.exists(this.name, false);
			if (s == null) {
				response = zk.create(this.name, bytes, 
						Ids.OPEN_ACL_UNSAFE, mode);
				isCreated = true;
			}
			isCreated = true;
		} catch (KeeperException e) {
			System.out.println("Failing when creating a node: " + this.name);
			e.printStackTrace();
			return null;
		} catch (InterruptedException e) {
			System.out.println("InterruptedException raised");
			return null;
		}

		return response;
	}

	public String assignWatcherChildren(Watcher watcher) {
		this.watcher = watcher;
		try {
			Stat s = zk.exists(this.name, this.watcher);
			List<String> list = zk.getChildren(this.name, this.watcher, s);
			return null;

		} catch (KeeperException e) {
			System.out.println("The session with Zookeeper failes. Closing");
			return null;
		} catch (InterruptedException e) {
			System.out.println("InterruptedException raised");
			return null;
		}

	}
	
	public List <String> getList() {
		if (!isCreated) return null;
		
		try {
			Stat s = zk.exists(this.name, this.watcher);
			List<String> list = zk.getChildren(this.name, this.watcher, s);
			printListMembers(list);		
			return list;

		} catch (KeeperException e) {
			System.out.println("The session with Zookeeper failes. Closing");
			return null;
		} catch (InterruptedException e) {
			System.out.println("InterruptedException raised");
			return null;
		}

	}

	private void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}

	public static void main(String[] args) {
		String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

		CreateSession cs = new CreateSession();
		ZooKeeper zk = cs.ConnectSession(hosts);
		if (zk == null) {
			System.out.println("No se ha creado la conexión");
			return;
		} else {
			System.out.println("Se ha creado la conexión");
		}

		String name = "/prueba";

		CreateMode mode =  CreateMode.PERSISTENT;
		CreateZNode createZnode = new CreateZNode(zk, name, new byte[0], mode);
		String myID = createZnode.createZNode();
		System.out.println(myID);

	}

}
