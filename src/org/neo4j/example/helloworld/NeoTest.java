package org.neo4j.example.helloworld;

import org.neo4j.graphdb.*;

import org.neo4j.kernel.EmbeddedGraphDatabase;
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class NeoTest {
 
    public enum MyRelationshipTypes implements RelationshipType {
        KNOWS
    }
 
    public static void main(String[] args) {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase("var/base");
 
 
        Transaction tx = graphDb.beginTx();
        try {
            Node firstNode = graphDb.createNode();
            Node secondNode = graphDb.createNode();
            Relationship relationship = firstNode.createRelationshipTo(secondNode, MyRelationshipTypes.KNOWS);
 
            System.out.println(firstNode.getId());
            firstNode.setProperty("message", "Hello, ");
            System.out.println(secondNode.getId());
            secondNode.setProperty("message", "world!");
            relationship.setProperty("message", "brave Neo4j ");
            tx.success();
 
            System.out.print(firstNode.getProperty("message"));
            System.out.print(relationship.getProperty("message"));
            System.out.println(secondNode.getProperty("message"));
        }
        finally {
            tx.finish();
        }

        System.out.println(graphDb.toString());
        graphDb.shutdown();
        
        
    }
}