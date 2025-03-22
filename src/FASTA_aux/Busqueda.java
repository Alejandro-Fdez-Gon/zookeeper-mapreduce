package es.upm.dit.cnvr_fcon.FASTA_aux;

import java.io.FileInputStream;
import java.io.Serializable;

import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import es.upm.dit.cnvr_fcon.FASTA_interface.BusquedaInterface;

public class Busqueda implements BusquedaInterface, Serializable{

	private static final long serialVersionUID = 1L;

	
	int indice;
	private byte[] patron;
	private byte[] subGenoma;

	public Busqueda() { 
		this.indice    = 0;
		this.patron    = null;
		this.subGenoma = null;
	}
	
	public Busqueda(String file) { 
		readBusquedaFile(file);
	}
	
	/**
	 * Constructor de la clase
	 * @param contenido Representación de un subgenoma
	 * @param patron El patrón a buscar
	 * @param indice La posición del subgenoma en la genoma global
	 */
	public Busqueda(byte[] contenido, byte[] patron, int indice) { 

		this.indice    = indice;
		this.patron    = patron;
		this.subGenoma = contenido;
	}

	/**
	 * Obtener el índice 
	 * @return El índice
	 */
	@Override
	public int getIndice() {
		return indice;
	}

	/**
	 * Actualizar el índice
	 * @param indice El valor
	 */
	@Override
	public void setIndice(int indice) {
		this.indice = indice;
	}

	/**
	 * Obtener el patrón
	 * @return el valor del patrón
	 */
	@Override
	public byte[] getPatron() {
		return patron;
	}

	/**
	 * Actualizar el patrón
	 * @param patron El valor
	 */
	@Override
	public void setPatron(byte[] patron) {
		this.patron = patron;
	}

	/**
	 * Obtener el subGenoma
	 * @return El valor
	 */
	@Override
	public byte[] getGenoma() {
		return subGenoma;
	}

	/**
	 * Actualizar el sub genoma
	 * @param subGenoma el valor
	 */
	@Override
	public void setSubGenoma(byte[] subGenoma) {
		this.subGenoma = subGenoma;
	}
	
	public boolean saveBusquedaFile(String filepath) {
		try {
			FileOutputStream fileOut = new FileOutputStream(filepath);
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(this);
			objectOut.close();
			//LOGGER.fine("The Object  was succesfully written to a file");
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			//LOGGER.severe("Unexpected file");
			return false;
		}
	}
	
	public boolean readBusquedaFile (String filepath) {
		
		try {
			FileInputStream fileIn = new FileInputStream(filepath);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Busqueda busquedaFich = new Busqueda();
            busquedaFich = (Busqueda) objectIn.readObject();
            //TODO
            // Por alguna razón, al recuperar el fichero se genera
            // una busqueda en la que se intercambia el patrón
            // con el subgenoma. Por esto se cambia
            // Pendiente: detectar el problema
            
            this.indice    = busquedaFich.indice;
    		this.patron    = busquedaFich.subGenoma;
    		this.subGenoma = busquedaFich.patron;
    		//System.out.println(this.patron.length);
    		//System.out.println(this.subGenoma.length);
            //LOGGER.fine("The Object has been read from the file");
            objectIn.close();
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			//LOGGER.severe("Unexpected file");
			return false;
		}		
	}
	
}


