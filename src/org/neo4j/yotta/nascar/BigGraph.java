package org.neo4j.yotta.nascar;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeSet;

import org.neo4j.graphdb.Transaction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.index.lucene.LuceneIndexBatchInserter;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.index.lucene.LuceneIndexService;

public class BigGraph {
	
	//public static String FILENAME = "textdb/db1.txt";
	//public static String DBDIR = "var/test";
	
	public static String FILENAME = "textdb/db2.txt";
	public static String DBDIR = "var/nascar_db2";
	
	LuceneIndexService index;
	GraphDatabaseService graphDb;
	
	public void insertDataForTest(String fileName, String dbDir) throws FileNotFoundException {
		int N = 1000000;
		int M = 100000000;

		Scanner in = new Scanner(new File(fileName));
		N = in.nextInt();
		M = in.nextInt();
		
		BatchInserter inserter = new BatchInserterImpl( dbDir, BatchInserterImpl.loadProperties( "neo4j.props" ) );
		LuceneIndexBatchInserter indexService = new LuceneIndexBatchInserterImpl( inserter );
		
		Map<String,Object> properties = new HashMap<String,Object>();

		for (int cnt = 0; cnt < N; cnt++) {
			int ind = in.nextInt();
			properties.put( "name", "" + ind );
			long node = inserter.createNode( properties );
			indexService.index( node, "name", "" + ind);
		}
		System.out.println("Done inserting nodes");
		
		indexService.optimize();
		System.out.println("Done optimizing");
		
		for (int i = 0; i < M; i++) {
			int n1 = in.nextInt();
			int n2 = in.nextInt();
		    long node1 = indexService.getNodes( "name", "" + n1).iterator().next();
		    long node2 = indexService.getNodes( "name", "" + n2).iterator().next();
		    
		    inserter.createRelationship( node1, node2, DynamicRelationshipType.withName( "KNOWS" ), null );
		    if(i % 100000 == 0)
		    	System.out.println("Done inserting " + i);
		}

		System.out.println("Done inserting edges");
		inserter.shutdown();
		System.out.println("Shutdown.");
	}
	
	public void stat(String dbDir) {
		graphDb = null;
		try {
	        graphDb = new EmbeddedGraphDatabase(dbDir);
	        Iterator<Node> allnodes = graphDb.getAllNodes().iterator();

	        int cnt = 0;
	        while(allnodes.hasNext()) {
	        	Node n = allnodes.next();
	        	cnt++ ;
	        	
	        	if(n != null) {
	        		try {
		        		if (n.getProperty("name") != null)
		        		System.out.println(n.getProperty("name"));
	        		} catch (Exception ex ) {
	        			ex.printStackTrace();
	        		}
	        	}
	        }
	        System.out.println("There are " + cnt + "nodes.");
	        
	        
	        for(int i = 1; i <= 10; i++) {
	        	Node n = graphDb.getNodeById(i);
	        	if (n != null)
	        	System.out.println(n.getProperty("name"));
	        }
	        
	        for(int i = 0; i < 30; i++) {
	        	Node[] nodes = graphDb.getRelationshipById(i).getNodes();
	        	if (nodes == null)
	        		continue;
	        	
	        	System.out.print("Edge " + i + " = ");
	        	for(Node n1 : nodes)
	        		System.out.print(n1.getProperty("name") + " " );
	        	System.out.println();
	        }
		} catch (NotFoundException ex) {
			ex.printStackTrace();
		} finally {
			if (graphDb != null)
				graphDb.shutdown();
		}
	}
	
	
	public void loadDbAndIndex(GraphDatabaseService graphDb) {
		index = new LuceneIndexService(graphDb);

		Transaction mytx = graphDb.beginTx();
		try {
			Iterator<Node> allnodes = graphDb.getAllNodes().iterator();
			
	        while(allnodes.hasNext()) {
	        	Node n = allnodes.next();
	        	
	        	if(n != null) {
	        		try {
		        		if (n.getProperty("name") != null) {
		        			index.index(n, "name", n.getProperty("name"));
		        		}
	        		} catch (Exception ex ) {
	        			ex.printStackTrace();
	        		}
	        	}
	        }
	        mytx.success();
		} catch(Exception ex) {
        	ex.printStackTrace();
        	System.err.println("it is not a single node.");
        } finally {
			if(mytx != null)
				mytx.finish();
		}
	}
	
	static class TopKResult {
		String name;
		double rank;
		public TopKResult(String n1, double r1) {
			rank = r1;
			name = n1;
		}
		
		public String toString() {
			return name + " " + rank;
		}
	}
	
	public void getTopK(String dbDir, String nascarName, int K) {
		graphDb = null;
		try {
			
			graphDb = new EmbeddedGraphDatabase(dbDir);
			loadDbAndIndex(graphDb);
			
			HashSet<String> commonNodes = new HashSet<String>();
			HashSet<String> commonNodes2 = new HashSet<String>();
			TreeSet<TopKResult> res = new TreeSet<BigGraph.TopKResult>(new Comparator<BigGraph.TopKResult>() {
				public int sign(double d) {
					if (d > 0) 
						return 1;
					else if(d < 0)
						return -1;
					else return 0;
				}
				
				public int compare(TopKResult o1, TopKResult o2) {
					return sign(-(o1.rank - o2.rank));
				}
			});
			
			commonNodes.clear();
			commonNodes2.clear();
			
			ArrayList<Node> commonNodeList = new ArrayList<Node>();
			Node nascar = null;
			try {
				nascar = index.getSingleNode("name", nascarName);
			} catch(Exception ex) {
				ex.printStackTrace();
				System.out.println("Cannot continue finding topK");
				return;
			}
			
			if(nascar != null) {
				Iterator<Relationship> allRel = nascar.getRelationships(Direction.OUTGOING).iterator();
				int cnt = 0;
				while(allRel.hasNext()) {
					Relationship rel = allRel.next();
					Node endNode = rel.getEndNode();
					commonNodeList.add(endNode);
					cnt ++;
					try {
						System.out.println(endNode.getProperty("name"));
						commonNodes.add(endNode.getProperty("name").toString());
					} catch(Exception ex) {
						ex.printStackTrace();
					}
				}
				
				for(Node n : commonNodeList) {
					allRel = n.getRelationships(Direction.INCOMING).iterator();
					System.out.println(n.getProperty("name").toString());
					while(allRel.hasNext()) {
						Relationship rel = allRel.next();
						Node startNode = rel.getStartNode();
						
						
						try {
							if(startNode.getProperty("name") != null && !commonNodes2.contains(startNode.getProperty("name").toString())) {
								commonNodes2.add(startNode.getProperty("name").toString());
								
								Iterator<Relationship>  allRel1 = startNode.getRelationships(Direction.OUTGOING).iterator();
								int commonCnt = 0;
								int cnt1 = 0;
								while(allRel1.hasNext()) {
									Relationship rel1 = allRel1.next();
									Node endNode = rel1.getEndNode();
									cnt1++;
									try {
										if (endNode.getProperty("name") != null) {
											if (commonNodes.contains(endNode.getProperty("name").toString())) {
												commonCnt++;
											}
										}
									} catch(Exception ex) {
										ex.printStackTrace();
									}
								}
								
								res.add(new TopKResult(startNode.getProperty("name").toString(), commonCnt*commonCnt*1./cnt/cnt1));
							}
						} catch(Exception ex) {
							ex.printStackTrace();
						}
					}
				}
			}
			
			System.out.println("***** RESULT *****");
			for(TopKResult it : res) {
				System.out.println(it);
			}
			
		} catch (NotFoundException ex) {
			ex.printStackTrace();
		} finally {
			if (graphDb != null)
				graphDb.shutdown();
		}
	}
	
	static public void main(String args[]) throws FileNotFoundException {
		
		BigGraph instance = new BigGraph();
		//instance.insertDataForTest(FILENAME, DBDIR);
		//instance.stat(DBDIR);
		instance.getTopK(DBDIR, "200", 2);
		
		System.out.println("done.");
	}
}
