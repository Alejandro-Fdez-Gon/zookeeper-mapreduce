package es.upm.dit.cnvr_fcon.FASTAMapReduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import FASTA_aux.Busqueda;
import FASTA_aux.FASTABuscar;
import FASTA_aux.Resultado;

import ZK.CreateSession;
import ZK.CreateZNode;

public class FASTAProcess implements Watcher {

	// Instancia de ZooKeeper para interactuar con el servidor ZooKeeper
	private ZooKeeper zk = null;

	// Identificador del miembro, ruta aboluta y simplificada
	private String myId;
	private String myName;

	// Nombres de los nodos principales en ZooKeeper
	private static String nodeMember  = "/members";
	private static String nodeAMember = "/member-";
	private String nodeComm     	  = "/comm";
	private String nodeSegment  	  = "/segments";
	private String nodeResult   	  = "/results";

	// Lista de direcciones de los servidores ZooKeeper a los que conectar
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};
	
	// Configuración del formato del logger y su creacion
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"[%1$tF %1$tT][%4$-7s] [%5$s] [%2$-7s] %n");
	}
	static final Logger LOGGER = Logger.getLogger(FASTAMapReduce.class.getName());

	// Constructor de la clase
	public FASTAProcess() {
		configurarLogger();
		create_ZK_Nodes();
		getSegment();
	}

	// Configuracion de un logger
	private void configurarLogger() {
		ConsoleHandler handler;
		handler = new ConsoleHandler(); 
		handler.setLevel(Level.FINEST); 
		LOGGER.addHandler(handler); 
		LOGGER.setLevel(Level.FINEST);
	}

	// Método que crea los nodos necesarios para procesar los segmentos
	private void create_ZK_Nodes() {
		CreateSession cs = new CreateSession();
		zk = cs.ConnectSession(hosts);

		if (zk == null) {
			LOGGER.severe("No se pudo establecer la conexión con ZooKeeper.");
			return;
		}

		LOGGER.info("Se ha creado la conexión");

		CreateMode modePers = CreateMode.PERSISTENT;
		CreateMode modeEphSeq = CreateMode.EPHEMERAL_SEQUENTIAL;

		CreateZNode createZnodeMem = new CreateZNode(zk, nodeMember, new byte[0], modePers);
		createZnodeMem.createZNode();

		CreateZNode createZnodeCom = new CreateZNode(zk, nodeComm, new byte[0], modePers);
		createZnodeCom.createZNode();

		String nameAMem = nodeMember + nodeAMember;
		CreateZNode createZnodeAMem = new CreateZNode(zk, nameAMem, new byte[0], modeEphSeq);
		myId = createZnodeAMem.createZNode();
		myName = myId.replace(nodeMember + "/", "");
		LOGGER.info("Nuevo nodo creado en " + nodeMember + " con nombre:" + myName);

		String nameACom = nodeComm + "/" + myName;
		CreateZNode createZnodeACom = new CreateZNode(zk, nameACom, new byte[0], modePers);
		createZnodeACom.createZNode();
		LOGGER.info("Nuevo nodo creado en " + nodeComm + " con nombre:" + myName);
	}

	// Metodo para obtener los segmentos a procesar
	private void getSegment(){
		Stat s = null;
		String nameACom = nodeComm + "/" + myName;
		try {
			List<String> list = zk.getChildren(nameACom, false, s);	
			Stat stat = zk.exists(nameACom + nodeSegment, watcherCommMember);
			if (!list.isEmpty() && stat != null) {
				LOGGER.info("Se ha actualizado los hijos de: " + nameACom);
				processSegment(nameACom + nodeSegment);
			}
		} catch (KeeperException e) {
			LOGGER.severe("KeeperException al obtener los hijos del nodo: " + e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.severe("InterruptedException al obtener los hijos del nodo: " + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al obtener los hijos del nodo: " + e.getMessage());
		}
	}
	
	// Metodo para procesar un segmento y generar los resultados
	private boolean processSegment(String path) {
		try {
			LOGGER.info("Iniciando el procesado de:" + path);
			byte[] dataBusq = zk.getData(path, false, null);
			String nameACom = nodeComm + "/" + myName;
			zk.delete(nameACom + nodeSegment, -1);
			ByteArrayInputStream InByteStream = new ByteArrayInputStream(dataBusq);
	        ObjectInputStream InObjectStream = new ObjectInputStream(InByteStream);
			Busqueda busqueda = (Busqueda) InObjectStream.readObject();
			InObjectStream.close();
			
			FASTABuscar buscar = new FASTABuscar(busqueda);
			ArrayList<Long> posiciones = buscar.buscar(busqueda.getPatron());
			Resultado resultado = new Resultado(posiciones, busqueda.getIndice());
			
			ByteArrayOutputStream OutByteStream = new ByteArrayOutputStream();
	        ObjectOutputStream OutObjectStream = new ObjectOutputStream(OutByteStream);
	        OutObjectStream.writeObject(resultado);
	        OutObjectStream.close();
	        byte[] dataRes = OutByteStream.toByteArray();
	        
	        CreateZNode createZnodeACom = new CreateZNode(zk, nameACom + nodeResult, dataRes, CreateMode.PERSISTENT);
	        createZnodeACom.createZNode();

			return true;

		} catch (KeeperException e) {
			LOGGER.severe("Error de Zookeeper al procesar el segmento: " + e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.severe("El hilo fue interrumpido mientras procesaba el segmento: " + e.getMessage());
		} catch (IOException e) {
			LOGGER.severe("Error de I/O al procesar el segmento: " + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al procesar el segmento: " + e.getMessage());
		}
		return false;
	}

	// Método auxiliar para imprimir la lista de miembros obtenida de ZooKeeper
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}

	// Implementación del método de la interfaz Watcher, que recibe los eventos de ZooKeeper
	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(nodeSegment, null);
			printListMembers(list);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}

	// Watcher para notificar cuando se ha creado o borrado un hijo en /comm/memberxx
	private Watcher watcherCommMember = new Watcher() {
		public void process(WatchedEvent event) {
			try {
				String nameACom = nodeComm + "/" + myName;
				LOGGER.info("Se ha actualizado los hijos de: " + nameACom);
				Stat stat = zk.exists(nameACom + nodeSegment, watcherCommMember);
				if (stat != null) {
					processSegment(nameACom + nodeSegment);
				}
			} catch (KeeperException e) {
				LOGGER.severe("KeeperException en el watcher: " + e.getMessage());
			} catch (InterruptedException e) {
				LOGGER.severe("InterruptedException en el watcher: " + e.getMessage());
			} catch (Exception e) {
				LOGGER.severe("Error inesperado en el watcher: " + e.getMessage());
			}
		}
	};

	// Método principal que inicia el proceso de FASTAProcess
	public static void main(String[] args) {
		FASTAProcess procesar = new FASTAProcess();
		try {
			Thread.sleep(600000);
		} catch (Exception e) {
			LOGGER.severe("Error inesperado durante la ejecución de FastaProcess: " + e.getMessage());
		}
	}
}
