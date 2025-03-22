package es.upm.dit.cnvr_fcon.FASTA_interface;

public interface BusquedaInterface {

	/**
	 * Obtener el índice 
	 * @return El índice
	 */
	int getIndice();

	/**
	 * Actualizar el índice
	 * @param indice El valor
	 */
	void setIndice(int indice);

	/**
	 * Obtener el patrón
	 * @return el valor del patrón
	 */
	byte[] getPatron();

	/**
	 * Actualizar el patrón
	 * @param patron El valor
	 */
	void setPatron(byte[] patron);

	/**
	 * Obtener el subGenoma
	 * @return El valor
	 */
	byte[] getGenoma();

	/**
	 * Actualizar el sub genoma
	 * @param subGenoma el valor
	 */
	void setSubGenoma(byte[] subGenoma);
	
	public boolean saveBusquedaFile(String filepath);
	public boolean readBusquedaFile (String filepath);



}