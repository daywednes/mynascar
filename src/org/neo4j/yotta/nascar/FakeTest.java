package org.neo4j.yotta.nascar;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 *
 * @author mdoan
 */
public class FakeTest {
    
    /**
     * Prints some data to a file using a BufferedWriter
     */
	
	public static int N = 100000; // M
	public static int M = 5000000; // M
	public static String FILENAME = "textdb/db2.txt";
	//public static Random ran = new Random(System.currentTimeMillis());
	public static Random ran = new Random(100000);
	
    public void writeToFile(String filename) {
    	
        
        PrintWriter out = null;	
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
            out.println(N + " " + M);
            for (int i = 1; i <= N; i++)
            	out.println(i);
            
            for (int i = 1; i <= M; i++) {
            	do {
	            	int node1 = ran.nextInt(N) + 1;
	            	int node2 = ran.nextInt(N) + 1;
	            	if(node1 != node2) {
	            		out.println(node1 + " " + node2);
	            		break;
	            	}
            	} while (true);
            }
            
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (out != null) {
                out.flush();
                out.close();
            }
        }
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        new FakeTest().writeToFile(FILENAME);
    }
}