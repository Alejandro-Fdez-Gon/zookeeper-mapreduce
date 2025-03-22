package es.upm.dit.cnvr_fcon.FASTA_aux;

import java.util.ArrayList;

import es.upm.dit.cnvr_fcon.FASTA_interface.LectorFASTAInterface;

public class FASTABuscar implements LectorFASTAInterface {

    /**
     * un array de bytes en donde cada byte representa el 
     * valor de un nucleótido (‘A’,’T’,’G’,’N’, …).
     */
    protected byte[] contenido;
    
    /**
     * Indica el número de bytes válidos del array. 
     * Siempre se cumpla: bytesValidos <= contenido.length. 
     */
    protected int bytesValidos;
   
    /**
     * Constructor con un genoma
     * @param bytesLeidos representa el genoma 
     */
    public FASTABuscar(BytesLeidos bytesLeidos) {
    	this.contenido    = bytesLeidos.getGenoma();
    	this.bytesValidos = bytesLeidos.getBytesValidos();
    }
 
    /**
     * Constructor con una búsqueda que representa un genoma
     * y un índice, cuando es un fragmento
     * @param busqueda El genoma 
     */
    public FASTABuscar(Busqueda busqueda) {
    	this.contenido    = busqueda.getGenoma();
    	this.bytesValidos = this.contenido.length;
    }
 
    /**
     * Extraer una secuencia concreta a partir de una posición.
     * @param posicionInicial Posición de inicio del patrón con nucleótidos 
     * @param tamano Número de nucleótidos solicitados
     * @return La secuencia de nucleótidos solicitado. Retorna null
     * si los parámetros no son correctos  
     */
    @Override
	public String getSecuencia (int posicionInicial, int tamano) {
    	if (posicionInicial+tamano >= bytesValidos)
    		return null;
    	return new String(contenido, posicionInicial, tamano);
    }
    
 
    /** Comprobar si un patrón está en una posición 
     * @param patron secuencia de bytes que representa el patrón de 
     * nucleótidos que se quiere comparar.
     * @param posicionAComparar Posición de inicio para la búsqueda del patrón
     * @return Indica si se ha encontrado el patrón
     * @throws FASTAException Si el tamaño del patrón es mayor que el número de nucleótidos
     * a partir del inicio de búsqueda
     */
    private boolean comparar(byte[] patron, int posicionAComparar) throws FASTAException {

    	if (posicionAComparar+patron.length > bytesValidos) {
    		throw new FASTAException("patron se sale del fichero FASTA");
    	}
    	for(int i = 0; i < patron.length; i++) {
    		//if(patron[i] != contenido[posicionAComparar + i] && Character.toUpperCase(patron[i]) != 'N' && Character.toUpperCase(contenido[posicionAComparar + i]) != 'N') {
    	    if(patron[i] != contenido[posicionAComparar + i] && Character.toUpperCase(patron[i]) != 'N') {//&& Character.toUpperCase(contenido[posicionAComparar + i]) != 'N') {
    	  		    			return false;
    		}
    	}
    	return true;
    }
    
    /** Buscar las posiciones donde se encuentra el patrón en el cromosoma
     * @param patron secuencia de bytes que representa el patrón de 
     * nucleótidos que vamos buscando en el cromosoma.
     * @return Lista con las posiciones donde se ha encontrado el patrón.
     */
    @Override
	public ArrayList<Long> buscar(byte[] patron) {
    	// TODO
    	ArrayList<Long> posiciones = new ArrayList<Long>();
    	try {
    		for (Integer i = 0; i<= bytesValidos-patron.length; i++) {
    			if(comparar(patron, i)) {
					posiciones.add((long) i);
    			}
    		}
		} catch (FASTAException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Algo te falla");
		}
    	return posiciones;
    }
    
    public static void main(String[] args) {
       	long t0 =System.currentTimeMillis();
       	
		String fichero        = "ref100.fa";
		//fichero = "chr19.fa";
		FASTALeerFichero leer   = new FASTALeerFichero(fichero);
		BytesLeidos bytesLeidos = leer.getBytesLeidos();
		
    	FASTABuscar cr = new FASTABuscar(bytesLeidos);
    	
		String patronS  = "TGAAGCTA";
		byte[] patron = patronS.getBytes();
    	
		ArrayList<Long> posiciones = cr.buscar(patron);

/*    	if (posiciones.size() > 0)
	    	for (Integer pos : posiciones){
	    		System.out.println("Encontrado "+patronS+" en "+pos+" : "+ cr.getSecuencia(pos, patron.length));
	    	}
    	else {
    		System.out.println("No he encontrado : "+patronS+" en ningun sitio");
    	}
		System.out.println("Tiempo total "+(System.currentTimeMillis()-t0));
*/
    }
}
