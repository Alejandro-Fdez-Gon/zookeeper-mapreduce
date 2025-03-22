package es.upm.dit.cnvr_fcon.FASTA_interface;

import java.util.List;

public interface LectorFASTAInterface {

	/**
	 * Extraer una secuencia concreta a partir de una posición.
	 * @param posicionInicial Posición de inicio del patrón con nucleótidos 
	 * @param tamano Número de nucleótidos solicitados
	 * @return La secuencia de nucleótidos solicitado. Retorna null
	 * si los parámetros no son correctos  
	 */
	String getSecuencia(int posicionInicial, int tamano);

	/** Buscar las posiciones donde se encuentra el patrón en el cromosoma
	 * @param patron secuencia de bytes que representa el patrón de 
	 * nucleótidos que vamos buscando en el cromosoma.
	 * @return Lista con las posiciones donde se ha encontrado el patrón.
	 */
	List<Long> buscar(byte[] patron);

}