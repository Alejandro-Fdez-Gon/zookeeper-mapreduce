package es.upm.dit.cnvr_fcon.FASTAMapReduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import FASTA_aux.Busqueda;
import FASTA_aux.BytesLeidos;
import FASTALeerFichero;
import FASTA_aux.Resultado;
import FASTA_interface.BusquedaInterface;
import FASTA_interface.ResultadoInterface;

import ZK.CreateSession;
import ZK.CreateZNode;

public class FASTAMapReduce implements Watcher{
	
	// Instancia de ZooKeeper para interactuar con el servidor ZooKeeper
	private ZooKeeper zk = null;

	/** 
	 * El objeto BytesLeidos obtenidos, con el genoma 
	 * y la cantidad de valores válidos
	 */ 
	private BytesLeidos rb;

	/**
	 * Número de fragmentos en el genoma (contenido). Será el 
	 * número de hebras que habrá que invocar para procesarlos,
	 * al usar el monitor 
	 */
	private int numFragmentos = 100;
	
	// Nombres de los nodos principales en ZooKeeper
	private static String nodeMember = "/members";
	private String nodeComm     	 = "/comm";
	private String nodeSegment  	 = "/segments";
	private String nodeResult  		 = "/results";

	// Atributo para almacenar el patron de busqueda
	private byte[] patron = null;

	// Atributo para contabilizar el numero de segmentos restantes
	private int indice = 0;

	// Atributos para saber que segmentos se hanprocesado
	private HashMap<Integer, Boolean> segmProcesados = new HashMap<>();
	private HashMap<String, Integer> segmAsignados = new HashMap<>();

	// Atributo para almacenar los resultados parciales
	private HashMap<Integer, ArrayList<Long>> resFinal = new HashMap<>();

	// Atributo para identificar los miembros creados y eliminados
	private List<String> previousChildren = new ArrayList<>();

	// Lista de direcciones de los servidores ZooKeeper a los que conectar
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	// Configuración del formato del logger y su creacion
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"[%1$tF %1$tT][%4$-7s] [%5$s] [%2$-7s] %n");
	}
	static final Logger LOGGER = Logger.getLogger(FASTAMapReduce.class.getName());

	/**
	 * Constructor que crea un cromosoma a partir de un fichero
	 * @param ficheroCromosoma Nombre del fichero con el genoma
	 * @param patron El patrñon que se debe buscar
	 */
	public FASTAMapReduce(String ficheroCromosoma, byte[] patron) {
		try {
			FASTALeerFichero leer = new FASTALeerFichero(ficheroCromosoma);
			this.rb = leer.getBytesLeidos();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		this.patron = patron;

		configurarLogger();
		create_ZK_Nodes();
		updateMembers();
	}

	// Configuracion de un logger
	private void configurarLogger() {
		ConsoleHandler handler;
		handler = new ConsoleHandler(); 
		handler.setLevel(Level.FINE); 
		LOGGER.addHandler(handler); 
		LOGGER.setLevel(Level.FINE);
	}

	// Método que crea los nodos necesarios para procesar los segmentos
	private void create_ZK_Nodes(){
		CreateSession cs = new CreateSession();
		zk = cs.ConnectSession(hosts);

		if (zk == null) {
			LOGGER.severe("No se pudo establecer la conexión con ZooKeeper.");
			return;
		}

		LOGGER.info("Se ha creado la conexión");

		CreateZNode createZnodeMem = new CreateZNode(zk, nodeMember, new byte[0], CreateMode.PERSISTENT);
		createZnodeMem.createZNode();

		CreateZNode createZnodeCom = new CreateZNode(zk, nodeComm, new byte[0], CreateMode.PERSISTENT);
		createZnodeCom.createZNode();
	}

	// Metodo que comprueba el estado de los miembros
	private void updateMembers () {
		Stat stat = null;
		Stat statCom = null;
		try {
			previousChildren = zk.getChildren(nodeMember, watcherMembers, stat);
			for (String newChild : previousChildren) {
				assignSegment("/" + newChild);
				List<String> list_com = zk.getChildren(nodeComm + "/" + newChild, watcherCommMember, statCom);
			}

		} catch (KeeperException e) {
			LOGGER.severe("KeeperException al obtener los hijos del nodo " + nodeMember + ":" + e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.severe("InterruptedException al obtener los hijos del nodo: " + nodeMember + ":" + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al obtener los hijos del nodo: " + nodeMember + ":" + e.getMessage());
		}
	}

	// Obtiene un resultado de /comm/memberx y procesa.
	private boolean getResult(String pathResult, String member) {	
		try {
			byte[] dataRes = zk.getData(pathResult, false, null);
			ByteArrayInputStream InByteStream = new ByteArrayInputStream(dataRes);
	        ObjectInputStream InObjectStream = new ObjectInputStream(InByteStream);
			Resultado resultado = (Resultado) InObjectStream.readObject();
			InObjectStream.close();

			segmProcesados.put(resultado.getIndice(), true);
			segmAsignados.remove(member);
			LOGGER.fine("Segmentos asignados procesandose: " + segmAsignados);

			ArrayList<Long> res = processResult(resultado);

			if (res != null) {
				deleteAll(nodeComm);
				deleteAll(nodeMember);
				LOGGER.info("Se han recuperado todos los resultados.");
				for (Long pos : res){
					System.out.println("Patron encontrado en la posición: " + pos);
				}
				System.exit(0);
			} else {
				LOGGER.info("Aun no estan todos los resultados");
			}
		
			return true;

		} catch (KeeperException e) {
			LOGGER.severe("Error de Zookeeper al obtener el resultado: " + e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.severe("El hilo fue interrumpido mientras se obtenia el resultado: " + e.getMessage());
		} catch (IOException e) {
			LOGGER.severe("Error de I/O al obtener el resultado: " + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al obtener el resultado: " + e.getMessage());
		}
		return false;
	}

	/**
	 * Procesa el resultado de búsquda. Los índices buscados en un
	 * subgenoma se refieren a un subgenoma. Hay que actualizar su
	 * posición en el genoma global 
	 * @param resultado El resultado de una búsqueda
	 * @return La lista actualizada
	 */
	private ArrayList <Long> processResult(Resultado resultado) {
		resFinal.put(resultado.getIndice(), resultado.getLista());
		LOGGER.fine("Resultados parciales: " + resFinal);
		
		if (resFinal.size() < numFragmentos) {
			return null;
		}

		ArrayList<Long> lista = new ArrayList<>();
		int tamanoSubGenoma = (rb.getGenoma().length / numFragmentos); 
		try {
			for (int i = 0; i < numFragmentos; i++) {
				List<Long> listaRelativa = resFinal.get(i);
				if (listaRelativa != null) {
			        for (long posRelativa : listaRelativa) {
			            long posGlobal = (i * tamanoSubGenoma) + posRelativa;
			            lista.add(posGlobal);
			        }
			    }
			}
			return lista;

		} catch (Exception e) {
			LOGGER.severe("Error al procesar los resultados: " + e.getMessage());
		}
		return null;
	}

	// Generate a segment and assigned it to a process
	private void assignSegment(String member) {
		byte[] subgenoma = null;
		Integer retryInd = retrySegment();
		Busqueda segment;

		if ((indice+1) <= numFragmentos) {
			subgenoma = getGenome(indice, patron);
			segment = new Busqueda(subgenoma, patron, indice);
		} else if (retryInd != null) {
			subgenoma = getGenome(retryInd, patron);
			segment = new Busqueda(subgenoma, patron, retryInd);
		} else {
			LOGGER.info("No hay más segmentos disponibles para asignar.");
			return ;
		}
		
		try {
			Thread.sleep(25);
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
			objectStream.writeObject(segment);
			objectStream.close();

			byte[] data_res = byteStream.toByteArray();
			String nameACom = nodeComm + member + nodeSegment;
			CreateZNode createZnodeACom = new CreateZNode(zk, nameACom, data_res, CreateMode.PERSISTENT);
			createZnodeACom.createZNode();
			LOGGER.info("Segmento asignado correctamente al miembro: " + member);
			
			segmAsignados.put(member, retryInd);
			if (retryInd != null){
				segmProcesados.put(retryInd, false);
				segmAsignados.put(member, retryInd);
			} else {
				segmProcesados.put(indice, false);
				segmAsignados.put(member, indice);
			}
			
			indice++;
		} catch (IOException e) {
			LOGGER.severe("Error de I/O al asignar el segmento: " + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al asignar el segmento: " + e.getMessage());
		}
	}

	// Comprobacion de la existencia de indices sin procesar ni asignar
	private Integer retrySegment() {
        if ((indice+1) <= numFragmentos) {
            return null;
        }

        for (Map.Entry<Integer, Boolean> segmento : segmProcesados.entrySet()) {
            int indSegmento = segmento.getKey();
            boolean isProcessed = segmento.getValue();

            if (!isProcessed && !segmAsignados.containsValue(indSegmento)) {
				return indSegmento;
            }
        }

        return null;
    }

	/**
	 * Genera un subArray que representa un subGenoma del genoma completo
	 * @param numSubGenoma Índice del subgenoma
	 * @param patron El patron a buscar
	 * @return El subgenoma
	 */
	private byte[] getGenome(int indice, byte[] patron) {
		int tamanoSubGenoma = (rb.getGenoma().length / numFragmentos); 
		int inicioSubGenoma = tamanoSubGenoma * indice ;
		int longitudSubGenoma;

		if ((indice + 1) != numFragmentos) {
			longitudSubGenoma = tamanoSubGenoma - 1 + patron.length;
		} else {
			longitudSubGenoma = tamanoSubGenoma - 1;
		}   	

		byte[] subGenoma = Arrays.copyOfRange(rb.getGenoma(), inicioSubGenoma, inicioSubGenoma + longitudSubGenoma);
		LOGGER.fine("obtenerSubGenoma: " + inicioSubGenoma + " " + longitudSubGenoma);
		return subGenoma;
	}
	
	// Funcion recursiva para borrar todos los nodos una vez obtenidos todos los resultados parciales.
	private void deleteAll(String path) {
		try {
			List<String> children = zk.getChildren(path, false);

	        for (String child : children) {
	            String childPath = path + "/" + child;
	            deleteAll(childPath);
	        }
	        zk.delete(path, -1);
	        LOGGER.info("Eliminado nodo: " + path);
			
		} catch (KeeperException e) {
			LOGGER.severe("Error de Zookeeper al borrar los nodos: " + e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.severe("El hilo fue interrumpido mientras se borraban los nodos: " + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al borrar los nodos " + e.getMessage());
		}
        
    }
	
	// Funcion encargada de comprobar si queda algun indice sin procesar cuando falla un FASTAProcess
	private void asignarPendientes(Boolean segmPendientes) {
		if (segmPendientes == false) {
			return ;
		}
		
		try {
			List<String> currentChildren  = zk.getChildren(nodeMember, false);
			String member;
			Stat tieneSeg;
			Stat tieneRes;
		
			for (String child : currentChildren) {
				member = nodeComm + "/" + child;
				 tieneSeg = zk.exists(member + nodeSegment, false);
				 tieneRes = zk.exists(member + nodeResult, false);

				if (tieneSeg == null && tieneRes == null) {
					assignSegment("/" + child);
				}
			}
		} catch (KeeperException e) {
			LOGGER.severe("KeeperException al asignar segmento tras borrado de miembro: " + e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.severe("InterruptedException al asignar segmento tras borrado de miembro: " + e.getMessage());
		} catch (Exception e) {
			LOGGER.severe("Error inesperado al asignar segmento tras borrado de miembro: " + e.getMessage());
		}
    }

	// Mostrar los hijos de un zNode, que se recibe en list
	private void printListMembers (List<String> list) {
		String out = "Remaining # members:" + list.size();
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			out = out + string + ", ";				
		}
		out = out + "\n";
		LOGGER.finest(out);
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(nodeSegment, null);//watcherSegment); //this);
			printListMembers(list);

		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}

	// Watcher para notificar cuando se ha creado o borrado un hijo en /comm/memberxx
	private Watcher watcherCommMember = new Watcher() {
		public void process(WatchedEvent event) {
			Stat statCom = null;

			String path = event.getPath();
			String member = path.replace(nodeComm, "");
			String resultPath = path + nodeResult;
			
			Integer retryInd = retrySegment();

			LOGGER.info("Se han modificado el nodo:" + path);

			try {
				Thread.sleep(25);
				Stat stat = zk.exists(path + nodeResult, false);
				if ((stat != null) && getResult(resultPath, member)) {
					zk.delete(resultPath, -1);
					
					if ((indice+1) <= numFragmentos || (retryInd != null)) {
						assignSegment(member);
					}
				}
				List<String> list_com = zk.getChildren(path, watcherCommMember, statCom);

			} catch (KeeperException e) {
				LOGGER.severe("KeeperException en el watcher de " + path + ": " + e.getMessage());
				retryInd = retrySegment();
				if (retryInd != null) {
					asignarPendientes(true);
				}		
			} catch (InterruptedException e) {
				LOGGER.severe("InterruptedException en el watcher de " + path + ": " + e.getMessage());
			} catch (Exception e) {
				LOGGER.severe("Error inesperado en el watcher de " + path + ": " + e.getMessage());
			}
		}
	};

	// Watcher para notificar cuando se ha creado o borrado un hijo en /members
	private Watcher watcherMembers = new Watcher() {
		public void process(WatchedEvent event) {
			LOGGER.info("Miembros actualizados en: " + nodeMember);
			Stat s = null;
			
			try {
				List<String> currentChildren  = zk.getChildren(nodeMember, false);
				Set<String> currentSet = new HashSet<>(currentChildren);
				Set<String> previousSet = new HashSet<>(previousChildren);
				Stat statCom = null;

				currentSet.removeAll(previousChildren);
				for (String newChild : currentSet) {
					assignSegment("/" + newChild);
					List<String> list_com = zk.getChildren(nodeComm + "/" + newChild, watcherCommMember, statCom);
				}

				previousSet.removeAll(currentChildren);
				for (String removedChild : previousSet) {
					Stat stat = zk.exists(nodeComm + "/" + removedChild + nodeSegment, false);
					LOGGER.info("Eliminando el nodo: " + nodeComm + "/" + removedChild );

					if (stat != null) {
						segmAsignados.remove("/" + removedChild);
						zk.delete(nodeComm + "/" + removedChild + nodeSegment, -1);
						LOGGER.info("Eliminando su segmento");
					}
					zk.delete(nodeComm + "/" + removedChild, -1);
					LOGGER.info("El nodo: " + nodeComm + "/" + removedChild + " ha sido eliminado");
				}

				previousChildren = new ArrayList<>(currentChildren);
				List<String> list = zk.getChildren(nodeMember, watcherMembers, s);
				
			} catch (KeeperException e) {
				LOGGER.severe("KeeperException en el watcher de " + nodeMember + ": " + e.getMessage());
			} catch (InterruptedException e) {
				LOGGER.severe("InterruptedException en el watcher de " + nodeMember + ": " + e.getMessage());
			} catch (Exception e) {
				LOGGER.severe("Error inesperado en el watcher de " + nodeMember + ": " + e.getMessage());
			}
		}
	};

	// Método principal que inicia el proceso de FASTAMapReduce
	public static void main(String[] args) {
		//String fichero = "ref100.fa";
		//String fichero = "chr19.fa";
		String fichero = "ref100K.fa";
		String patronS  = "TGAAGCTA";
		byte[] patron = patronS.getBytes();
		FASTAMapReduce generar = new FASTAMapReduce(fichero, patron);

		try {
			Thread.sleep(600000);
		} catch (Exception e) {
			LOGGER.severe("Error inesperado durante la ejecución de FASTAMapReduce: " + e.getMessage());
		}
	}
}
