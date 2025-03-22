package es.upm.dit.cnvr_fcon.FASTA_aux;

import java.io.Serializable;

public class BytesLeidos implements Serializable{
	
	static final long serialVersionUID = 1L;
    /**
     * un array de bytes en donde cada byte representa el 
     * valor de un nucleótido (‘A’,’T’,’G’,’N’, …).
     */
    protected byte[] genoma;
    
    /**
     * Indica el número de bytes válidos del array. 
     * Siempre se cumpla: bytesValidos menor o igual del contenido. 
     */
    protected int bytesValidos;

	
	/**
	 * Constructor
	 * @param genoma El genoma representado en un array de bytes
	 * @param byteValidos El número de caracteres válidos
	 */
	public BytesLeidos(byte[] genoma, int byteValidos) {
		this.genoma=genoma;
		this.bytesValidos=byteValidos;
	}
	
	/**
	 * El genoma representado en un array de bytes en donde 
	 * cada byte representa el valor de un nucleótido (‘A’,’T’,’G’,’N’, …).
	 * @return El genoma
	 */
	public byte[] getGenoma() {
		return genoma;
	}
	
	/**
	 * el número de bytes válidos recibidos.
	 * @return El número
	 */
	public int getBytesValidos() {
		return bytesValidos;
	}
}
