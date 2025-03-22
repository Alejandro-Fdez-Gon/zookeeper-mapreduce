package es.upm.dit.cnvr_fcon.FASTA_aux;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import FASTA_interface.ResultadoInterface;

public class Resultado implements ResultadoInterface, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * La lista que incluirá las posiciones del patrón
	 */
	private ArrayList<Long> lista;
	
	/**
	 * Índice de la posición del subgenoma en el genoma global
	 */
	private int indice;
	
	public Resultado () {
		this.lista  = null;
		this.indice = 0;
	}
	
	/**
	 * Constructor de la clase
	 * @param lista La lista que incluirá las posiciones del patrón
	 * @param indice Índice de la posición del subgenoma en el genoma global
	 */
	public Resultado (ArrayList<Long> lista, int indice) {
		this.lista  = lista;
		this.indice = indice;
		
	} 

	/**
	 * Obtener la lista 
	 * @return La lista 
	 */
	@Override
	public ArrayList<Long> getLista() {
		return lista;
	}

	/**
	 * Actualizar la lista del objeto
	 * @param lista La lista a actualizar
	 */
	@Override
	public void setLista(ArrayList<Long> lista) {
		this.lista = lista;
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
	 * Actualizar el índice del objeto
	 * @param indice El índice a actualizar
	 */
	@Override
	public void setIndice(int indice) {
		this.indice = indice;
	}
	
	public boolean saveDBFile(String filepath) {
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
	
	public boolean getDBFile (String filepath) {
		
		try {
			FileInputStream fileIn = new FileInputStream(filepath);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Resultado resultadoFich = new Resultado();
            resultadoFich = (Resultado) objectIn.readObject();
            //LOGGER.fine("The Object has been read from the file");
            this.indice = resultadoFich.indice;
            this.lista  = resultadoFich.lista;
            objectIn.close();
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			//LOGGER.severe("Unexpected file");
			return false;
		}		
	}
}
