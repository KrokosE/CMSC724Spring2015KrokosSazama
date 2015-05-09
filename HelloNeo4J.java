 import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.ArrayList;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.cypher.internal.compiler.v2_2.functions.Rels;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.impl.util.StringLogger;

public class HelloNeo4J {
    private static final String DB_PATH = "./Neo4j/";
    
    static int numberOfNodes = 50000;
    static String fileSet = "N50000k5m20";
    static int numberOfRuns = 4;
    
    GraphDatabaseService db;
    
    Relationship myRelationship;
    
    public enum Relationships implements RelationshipType{
    	Connected
    }
    
    public static void main( final String[] args )
    {
        HelloNeo4J myNeoInstance = new HelloNeo4J();
        
        System.out.println("Started");

        ArrayList<String> nodeIds = GetSomeVertices(0.1f, numberOfNodes);
        System.out.println("Query1: " + runQuery1(myNeoInstance,nodeIds)+"ms");
        
        System.out.println("Query2: " + runQuery2(myNeoInstance,nodeIds)+"ms");

        System.out.println("Query3: " + runQuery3(myNeoInstance,nodeIds)+"ms");
                
        System.out.println("Query4: " + runQuery4(myNeoInstance,nodeIds)+"ms");
        
        System.out.println("Query5: " + runQuery5(myNeoInstance,nodeIds)+"ms");
        
        System.out.println("Query6: " + runQuery6(myNeoInstance,nodeIds)+"ms");
        
        ArrayList<String> nodePairs = GetSomePairs(0.1f, numberOfNodes);
        System.out.println("Query7: " + runQuery7(myNeoInstance,nodePairs)+"ms");
        
        ArrayList<String> newNodeConnections = GetSomeNewerPairs(0.1f, numberOfNodes);
        System.out.println("Query8: " + runQuery8(myNeoInstance,newNodeConnections)+"ms");
        
        myNeoInstance.shutDown();
        
        removeDirectory(new File(DB_PATH));
        removeDirectory(new File(DB_PATH+"../Neo4jTemp/"));
        
    }
    
    static boolean removeDirectory(File directory) {

    	  // System.out.println("removeDirectory " + directory);

    	  if (directory == null)
    	    return false;
    	  if (!directory.exists())
    	    return true;
    	  if (!directory.isDirectory())
    	    return false;

    	  String[] list = directory.list();

    	  // Some JVMs return null for File.list() when the
    	  // directory is empty.
    	  if (list != null) {
    	    for (int i = 0; i < list.length; i++) {
    	      File entry = new File(directory, list[i]);

    	      //        System.out.println("\tremoving entry " + entry);

    	      if (entry.isDirectory())
    	      {
    	        if (!removeDirectory(entry))
    	          return false;
    	      }
    	      else
    	      {
    	        if (!entry.delete())
    	          return false;
    	      }
    	    }
    	  }

    	  return directory.delete();
    	}
    
    static ArrayList<Node> GetNodes(ArrayList<String> nodeIds, HashMap<String,Node> nodes)
    {
    	ArrayList<Node> returnNodes = new ArrayList<Node>();
    	for(String id : nodeIds)
    	{
    		returnNodes.add(nodes.get(id));
    	}
    	
    	return returnNodes;
    }
    
    static ArrayList<String> GetSomeVertices(float percent, int totalNumber)
    {
    	ArrayList<String> nodeIDs = new ArrayList<String>();
    	int numberNeeded = (int) (totalNumber*percent);
    	
    	Random rand = new Random();
    	int min = 1;
    	int max = numberNeeded;
    	for(int i = 0; i < numberNeeded; i++)
    	{
    		int randomNum = rand.nextInt((max - min) + 1) + min;
    		nodeIDs.add(randomNum+"");
    	}
    	
    	return nodeIDs;
    }
    
    static ArrayList<String> GetSomePairs(float percent, int totalNumber)
    {
    	ArrayList<String> pair1 = GetSomeVertices(percent, totalNumber);
    	ArrayList<String> pair2 = GetSomeVertices(percent, totalNumber);
    	
    	ArrayList<String> pairs = new ArrayList<String>();
    	for(int i = 0; i < pair1.size(); i++)
    	{
    		pairs.add(pair1.get(i)+" "+ pair2.get(i));
    	}
    	
    	return pairs;
    }
    
    static ArrayList<String> GetSomeNewerNumbers(float percent, int totalNumber)
    {
    	ArrayList<String> nodeIDs = new ArrayList<String>();
    	int numberNeeded = totalNumber+1+(int) (totalNumber*percent);
    	
    	Random rand = new Random();
    	int min = totalNumber+1;
    	int max = numberNeeded;
    	for(int i = 0; i < numberNeeded; i++)
    	{
    		int randomNum = rand.nextInt((max - min) + 1) + min;
    		nodeIDs.add(randomNum+"");
    	}
    	
    	return nodeIDs;
    }
    
    static ArrayList<String> GetSomeNewerPairs(float percent, int totalNumber)
    {
    	ArrayList<String> pair1 = GetSomeNewerNumbers(percent, totalNumber);
    	ArrayList<String> pair2 = GetSomeNewerNumbers(percent, totalNumber);
    	
    	ArrayList<String> pairs = new ArrayList<String>();
    	for(int i = 0; i < pair1.size(); i++)
    	{
    		pairs.add(pair1.get(i)+" "+ pair2.get(i));
    	}
    	
    	return pairs;
    }
    
    HashMap<String,Node> CreateNeo4jWithData(String databasePath,int numberOfNodes, String path)
    {
    	ArrayList<Node> returnNodes = new ArrayList<Node>();
    	GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
    	db = dbFactory.newEmbeddedDatabase(DB_PATH);
    	
    	HashMap<String, Node> nodes = new HashMap<String, Node>();
		try (Transaction tx = db.beginTx()) 
		{
	    	for(int i = 1; i <= numberOfNodes; i++)
	    	{
	    		Node v = db.createNode(DynamicLabel.label(i+""));
	    		v.addLabel(DynamicLabel.label("color"));
	    		v.addLabel(DynamicLabel.label("label"));
	    		v.setProperty("color", "red");
	    		v.setProperty("label", i%4);
	    		nodes.put(i+"", v);
	    		returnNodes.add(v);

	    	}
	    	tx.success();
		}
		
		try (Transaction tx = db.beginTx()) 
		{
    	try(BufferedReader br = new BufferedReader(new FileReader(path))) {
    		for(String line; (line = br.readLine()) != null; ) {

    			String[] vertices = line.split("	");
    			String vertex1 = vertices[0];
    			String vertex2 = vertices[1];
    			vertex2 = vertex2.replaceAll(" ", "");
    			


    				Node v1 = nodes.get(vertex1);
    				Node v2 = nodes.get(vertex2);
    				Relationship relationship = v1.createRelationshipTo(v2,
    						Relationships.Connected);
 
    			}	
    		}
			tx.success();
    	}
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}
    	
    	return nodes;
    }
    
    //Retrieve nodes based on the node ID for 10% of the nodes 
	// in the database that have been previously randomly selected
	long query1Neo4j(ArrayList<String> inputNodes)
	{
		String nodeIds = "";
		for(int i = 0; i < inputNodes.size(); i++)
		{
			nodeIds += inputNodes.get(i);
			if(i < inputNodes.size()-1)
				nodeIds +=",";
		}
		
    	long startTime = System.nanoTime();
		try (Transaction ignored = db.beginTx();
				Result result = db.execute("START n = node(" + nodeIds	+ ") RETURN n;")) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Entry<String, Object> column : row.entrySet()) {
				//	System.out.println(column.getKey() + ": "
			//+ ((Node)(column.getValue())).getId());
				}
			}
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
	static long runQuery1(HelloNeo4J myNeoInstance, ArrayList<String> inputNodes)
	{
		long totalTime = 0;
		for(int x =0; x < numberOfRuns; x++)
		{
	        HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        totalTime += myNeoInstance.query1Neo4j(inputNodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
		}
		
		return totalTime/numberOfRuns;
	}
	
    //Retrieve nodes based on the node ID for 10% of the nodes 
    //in the database that have been previously randomly selected, 
    //group by 10% of the number of nodes being retrieved
    long query2Neo4j(ArrayList<String> inputNodes)
    {
		String nodeIds = "";
		for(int i = 0; i < inputNodes.size(); i++)
		{
			nodeIds += inputNodes.get(i);
			if(i < inputNodes.size()-1)
				nodeIds +=",";
		}
    	long startTime = System.nanoTime();
		try (Transaction ignored = db.beginTx();
				/*Result result = db.execute("START n = node(" + nodeIds
						//+ ") MATCH r, x, y, z"
						//+ " where r.label = 0 and id(r) in [" + nodeIds + "] and x.label = 1 "
						//+ "and id(x) in [" + nodeIds + "] and y.label = 2 and id(y) in [" + nodeIds + "] and"
						//+ " z.label = 3 and id(z) in [" + nodeIds + "] RETURN r, r.label, x, x.label, y, y.label,"
						//		+ " z, z.label;")) {
						+ ") MATCH r"
						+ " where r.label = 2 and id(r) in [" + nodeIds + "] "
								+ " RETURN r, r.label;")) */
				Result result = db.execute("START n = node(" + nodeIds	+ ") RETURN n ORDER BY n.label;")) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Entry<String, Object> column : row.entrySet()) {
					//System.out.println(column.getKey() + ": "
			//+ ((Node)(column.getValue())).getId());
				}
			}
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;

	}
    
    static long runQuery2(HelloNeo4J myNeoInstance, ArrayList<String> inputNodes)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
	    	HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        totalTime += myNeoInstance.query2Neo4j(inputNodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
    	}
    	return totalTime / numberOfRuns;
    }
    
    //Perform Breadth-First Search to level 2 from a given node 
    //for 10% of the nodes in the database that have been previously 
    //randomly selected
	long query3Neo4j(ArrayList<Node> nodes) 
	{
    	long startTime = System.nanoTime();
		for(Node n : nodes)
		{
			String output = "";
			Transaction ignored = db.beginTx();
			for ( Path position : db.traversalDescription()
			        .breadthFirst()
			        .evaluator( Evaluators.toDepth( 2 ) )
			        .traverse( n ) )
			{
			    output += position + "\n";
			}
			//System.out.println(output);
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
	static long runQuery3(HelloNeo4J myNeoInstance, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        ArrayList<Node> inputNodes = GetNodes(nodeIds, nodes);
	        totalTime += myNeoInstance.query3Neo4j(inputNodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
		}
		return totalTime / numberOfRuns;
	}
	
    //Perform Breadth-First Search to level 4 from a given node 
    //for 10% of the nodes in the database that have been previously 
    //randomly selected
	long query4Neo4j(ArrayList<Node> nodes)
	{
    	long startTime = System.nanoTime();
		for(Node n : nodes)
		{
			String output = "";
			Transaction ignored = db.beginTx();
			for ( Path position : db.traversalDescription()
			        .breadthFirst()
			        .evaluator( Evaluators.toDepth( 4 ) )
			        .traverse( n ) )
			{
			    output += position + "\n";
			}
			//System.out.println(output);
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
	static long runQuery4(HelloNeo4J myNeoInstance, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        ArrayList<Node> inputNodes = GetNodes(nodeIds, nodes);
	        totalTime += myNeoInstance.query4Neo4j(inputNodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
		}
		return totalTime / numberOfRuns;
	}
	
	
    //Compute Connected Components of the graph using outgoing edges
	long query5Neo4j(ArrayList<Node> nodes)
	{
		ArrayList<Node> methodNodes = (ArrayList<Node>) nodes.clone();
    	long startTime = System.nanoTime();
		int connectedComponent = 0;
		for(int i = 0; i < methodNodes.size(); i++)
		{
			Node n = methodNodes.get(i);
			String output = "";
			Transaction ignored = db.beginTx();
			for ( Path position : db.traversalDescription()
			        .breadthFirst()
			        .traverse( n ) )
			{
				methodNodes.remove(position.endNode());
			    output += position.endNode() + "\n";
			}
			//System.out.println("Connected Component " + connectedComponent);
			connectedComponent++;
			//System.out.println(output);
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
	static long runQuery5(HelloNeo4J myNeoInstance, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        ArrayList<Node> inputNodes = GetNodes(nodeIds, nodes);
	        totalTime += myNeoInstance.query5Neo4j(inputNodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
		}
		return totalTime / numberOfRuns;
	}
	
	
    //Compute Connected Components of the graph using incoming edges
	long query6Neo4j(ArrayList<Node> nodes) 
	{
		ArrayList<Node> methodNodes = (ArrayList<Node>) nodes.clone();
    	long startTime = System.nanoTime();
		int connectedComponent = 0;
		for(int i = 0; i < methodNodes.size(); i++)
		{
			Node n = methodNodes.get(i);
			String output = "";
			Transaction ignored = db.beginTx();
			for ( Path position : db.traversalDescription()
			        .breadthFirst()
			        .reverse()
			        .traverse( n ) )
			{
				methodNodes.remove(position.endNode());
			    output += position.endNode() + "\n";
			}
			//System.out.println("Connected Component " + connectedComponent);
			connectedComponent++;
			//System.out.println(output);
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
	static long runQuery6(HelloNeo4J myNeoInstance, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        ArrayList<Node> inputNodes = GetNodes(nodeIds, nodes);
	        totalTime += myNeoInstance.query6Neo4j(inputNodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
		}
		return totalTime / numberOfRuns;
	}
	
	//Compute Shortest Path for a pair of nodes for 10% of the 
	//nodes in the database that have been previously randomly selected
	long query7Neo4j(ArrayList<String> nodeIds, HashMap<String,Node> nodes)
	{
		long startTime = System.nanoTime();
		for(String ids : nodeIds)
		{
			String[] idPair = ids.split(" ");
			String firstVertex = idPair[0];
			String secondVertex = idPair[1];
			
			String output = "";
			try (Transaction ignored = db.beginTx();
					Result result = db.execute("START source=node(" + nodes.get(firstVertex).getId() +
							"), destination=node(" + nodes.get(secondVertex).getId() 
							+ ") MATCH p = shortestPath((source)-[*..100]-(destination))"
							+ " RETURN p")) {
				while (result.hasNext()) {
					output += result.next() + "1\n";
				}
			}
			//System.out.println(output);
		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
	static long runQuery7(HelloNeo4J myNeoInstance, ArrayList<String> nodePairs)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Node> nodes = myNeoInstance.CreateNeo4jWithData(DB_PATH,numberOfNodes,
	        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
	        totalTime += myNeoInstance.query7Neo4j(nodePairs,nodes);
	        myNeoInstance.shutDown();
	        removeDirectory(new File(DB_PATH));
		}
		return totalTime / numberOfRuns;
	}
	
	ArrayList<Long> modificationQueries(int numberOfNewNodes, ArrayList<String> connections, int numberOfNodes, String path)
	{
		GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
		GraphDatabaseService localDB = dbFactory.newEmbeddedDatabase(DB_PATH+"../Neo4jTemp/");
    	//Create the orig database
    	HashMap<String, Node> nodes = new HashMap<String, Node>();
		try (Transaction tx = localDB.beginTx()) 
		{
	    	for(int i = 1; i <= numberOfNodes; i++)
	    	{
	    		Node v = localDB.createNode(DynamicLabel.label(i+""));
	    		v.addLabel(DynamicLabel.label("color"));
	    		v.addLabel(DynamicLabel.label("label"));
	    		v.setProperty("color", "red");
	    		v.setProperty("label", i%4);
	    		nodes.put(i+"", v);
	    	}
	    	tx.success();
		}		
    	try(BufferedReader br = new BufferedReader(new FileReader(path))) {
    		for(String line; (line = br.readLine()) != null; ) {

    			String[] vertices = line.split("	");
    			String vertex1 = vertices[0];
    			String vertex2 = vertices[1];
    			vertex2 = vertex2.replaceAll(" ", "");
    			
    			try (Transaction tx = localDB.beginTx()) 
    			{

    				Node v1 = nodes.get(vertex1);
    				Node v2 = nodes.get(vertex2);
    				Relationship relationship = v1.createRelationshipTo(v2,
    						Relationships.Connected);
    				tx.success();
    			}	
    		}
    	}
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}
    	
    	ArrayList<Long> times = new ArrayList<Long>();
    	
    	
	  	//Insert new nodes
    	ArrayList<Node> newVerts = new ArrayList<Node>();
    	long startTime = System.nanoTime();
    	try (Transaction tx = localDB.beginTx()) 
    	{
    		for(int i = numberOfNodes+1; i <= numberOfNodes+numberOfNewNodes; i++)
    		{
    			Node v = localDB.createNode(DynamicLabel.label(i+""));
    			v.addLabel(DynamicLabel.label("color"));
    			v.addLabel(DynamicLabel.label("label"));
    			v.setProperty("color", "red");
    			v.setProperty("label", i%4);
    			newVerts.add(v);
	    		nodes.put(i+"", v);
    		}
    		tx.success();
    	}

		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);
    	
    	
    	
    	//Insert Edges
    	ArrayList<Relationship> edges = new ArrayList<Relationship>();
    	startTime = System.nanoTime();
    	try (Transaction tx = localDB.beginTx()) 
    	{
    		for(String line : connections ) 
    		{

    			String[] vertices = line.split(" ");
    			String vertex1 = vertices[0];
    			String vertex2 = vertices[1];
    			vertex2 = vertex2.replaceAll(" ", "");

    			Node v1 = nodes.get(vertex1);
    			Node v2 = nodes.get(vertex2);

    			Relationship relationship = v1.createRelationshipTo(v2,
    					Relationships.Connected);
    			edges.add(relationship);
    		}
    		tx.success();
    	}

    	endTime = System.nanoTime();
    	durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);
    	
    	
    	//Delete edges
    	startTime = System.nanoTime();
    	try (Transaction tx = localDB.beginTx()) 
    	{
    		for(int i = 0; i < edges.size(); i++)
    		{
    			edges.get(i).delete();
    		}
    		tx.success();
    	}
    	endTime = System.nanoTime();
    	durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);

    	
    	//Delete vertices
    	startTime = System.nanoTime();
    	try (Transaction tx = localDB.beginTx()) 
    	{
    		for(int i = 0; i < newVerts.size(); i++)
    		{
    			newVerts.get(i).delete();
    		}
    		tx.success();
    	}
    	endTime = System.nanoTime();
    	durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);

    	localDB.shutdown();
    	
    	return times;
	}
	 
	static ArrayList<Long> runQuery8(HelloNeo4J myNeoInstance, ArrayList<String> connections)
	{
		ArrayList<Long> times = new ArrayList<Long>();
		times.add((long) 0);
		times.add((long) 0);
		times.add((long) 0);
		times.add((long) 0);
		for(int x = 0; x < numberOfRuns; x++)
		{
		
			ArrayList<Long> newTimes = myNeoInstance.modificationQueries(connections.size(), connections,numberOfNodes,
        		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			for(int i = 0; i < newTimes.size(); i++)
			{
				times.set(i, times.get(i) + newTimes.get(i));
			}
	        removeDirectory(new File(DB_PATH+"../Neo4jTemp/"));
		}
		
		for(int i = 0; i < times.size(); i++)
		{
			times.set(i, times.get(i)/numberOfRuns);
		}
		
		return times;
	}
	
    void removeData()
    {
    }
    
    void shutDown()
    {
        db.shutdown();
    }   
}

