package es.upm.dit.cnvr_fcon.FASTA_interface;

import java.util.ArrayList;

public interface ResultadoInterface {

	/**
	 * Obtener la lista 
	 * @return La lista 
	 */
	ArrayList<Long> getLista();

	/**
	 * Actualizar la lista del objeto
	 * @param lista La lista a actualizar
	 */
	void setLista(ArrayList<Long> lista);

	/**
	 * Obtener el índice
	 * @return El índice
	 */
	int getIndice();

	/**
	 * Actualizar el índice del objeto
	 * @param indice El índice a actualizar
	 */
	void setIndice(int indice);

}