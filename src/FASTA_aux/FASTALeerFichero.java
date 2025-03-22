package es.upm.dit.cnvr_fcon.FASTA_aux;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import es.upm.dit.cnvr_fcon.FASTA_aux.Busqueda;
import es.upm.dit.cnvr_fcon.FASTA_aux.BytesLeidos;

public class FASTALeerFichero {
	
	/** 
	 * El objeto BytesLeidos obtenidos, con el genoma 
     * y la cantidad de valores válidos
     */ 
	private BytesLeidos rb;
    
    /**
     * Crear un objeto y lee el genoma desde un fichero
     * @param ficheroGenoma El nombre del fichero
     */
    public FASTALeerFichero(String ficheroGenoma) {
		try {
			this.rb = leerFichero(ficheroGenoma);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
    }

    /** 
     * Este método lee un fichero y genera un genoma en contenido 
     * @param fileName el fichero de entrada
     * @return La representación del genoma
     * @throws IOException as
     */
    private BytesLeidos leerFichero(String fileName) throws IOException {
    	File f=new File(fileName);
    	FileInputStream fis=new FileInputStream(f);
    	DataInput fid=new DataInputStream(fis);
		long len=(int) fis.getChannel().size();
		if (len > Integer.MAX_VALUE) {
			fis.close();
			throw new IOException("El fichero "+fileName+" es muy grande. No podemos representarlo con un array.");
		}
    	byte[] contents=new byte[(int) len];
    	int bytesRead=0;
    	int numRead=0;
    	//int cont = 0;
    	String line;
    	while ((line=fid.readLine()) != null) {
    		line=line.toUpperCase();
    		numRead=line.length();
    		byte[] newData=line.getBytes();
    		for (int i=0; i < numRead; i++)
    			contents[bytesRead+i]=newData[i];
    		bytesRead+=numRead;
    		//cont = cont + 1;
    		//System.out.println(cont);
    	}
	    fis.close();
	    return new BytesLeidos(contents,bytesRead);
    }

    /**
     * Obtener el objeto BytesLeidos obtenidos, con el genoma 
     * y la cantidad de valores válidos
     * @return El objeto obtenido
     */
    public BytesLeidos getBytesLeidos() {
    	return this.rb;
    }
    
	public static void main(String[] args) {
		
		// Obtener un genoma (byte[]) a partir de un fichero
		
		String fichero = "ref100.fa";
		FASTALeerFichero generar = new FASTALeerFichero(fichero);
		byte[] genoma = generar.getBytesLeidos().getGenoma();
	}
	
}
