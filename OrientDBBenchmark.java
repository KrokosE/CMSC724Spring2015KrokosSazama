 import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;






import java.util.Random;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OrientDBBenchmark {
    private static final String ORIENT_DB_PATH = "./OrientData/";

    OrientGraphNoTx orientDBGraph;
  	ODatabaseDocumentTx odb;
  	OrientGraphFactory factory;
  	
    static int numberOfNodes = 10000;
    static String fileSet = "N1000010m30";
    static int numberOfRuns = 4;
    
    HashMap<String, Integer> clusters;

     
    public static void main( final String[] args )
    {
    
        OrientDBBenchmark myOrientInstance = new OrientDBBenchmark();    
        
        PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream("output.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.setOut(out);
        
        File file = new File("./DirectedDataSets/DirectedDataSets/");
        String[] names = file.list();
        for(String name : names)
        {
            if (new File("./DirectedDataSets/DirectedDataSets/" + name).isDirectory())
            {
            	fileSet = name;
            	numberOfNodes = Integer.parseInt(name.substring(1, name.indexOf("k")));
                System.out.println(name);

                ArrayList<String> nodeIds = GetSomeVertices(0.1f, numberOfNodes);
		        
		        //System.out.println(name+": Query1 Time: " + runQuery1(myOrientInstance, nodeIds)+"ms");
		        System.out.println("Query2 Time: " + runQuery2(myOrientInstance, nodeIds)+"ms");
		        System.out.println(name+": Query3 Time: " + runQuery3(myOrientInstance, nodeIds)+"ms");
		        System.out.println(name+": Query4 Time: " + runQuery4(myOrientInstance, nodeIds)+"ms");
		        System.out.println(name+": Query5 Time: " + runQuery5(myOrientInstance, nodeIds)+"ms");
		        System.out.println(name+": Query6 Time: " + runQuery6(myOrientInstance, nodeIds)+"ms");
		
		        
		        ArrayList<String> nodePairs = GetSomePairs(0.1f, numberOfNodes);
		        System.out.println(name+": Query7 Time: " +runQuery7(myOrientInstance,nodePairs)+"ms");
		        
		        ArrayList<String> newNodeConnections = GetSomeNewerPairs(0.1f, 1000);
		        System.out.println(name+": Query8 Time: " +runQuery8(myOrientInstance,newNodeConnections)+"ms");
		        
		        myOrientInstance.shutDown();

            }
        }
        
    }

    HashMap<String, Vertex> CreateOrientDBWithData(String databasePath,int numberOfNodes, String path)
    {
    	HashMap<String, Vertex> returnNodes = new HashMap<String, Vertex>();
    	factory = new OrientGraphFactory("plocal:"+ORIENT_DB_PATH);
    	orientDBGraph = factory.getNoTx();
    	odb = orientDBGraph.getRawGraph();
    	   	    
    	HashMap<String, Vertex> nodes = new HashMap<String, Vertex>();

    	for(int i = 1; i <= numberOfNodes; i++)
    	{
    		Vertex v = orientDBGraph.addVertex("class:Vertex");
    		v.setProperty("vertexID", i+"");
    		v.setProperty("Attribute", i%4);
    		nodes.put(i+"", v);
    		returnNodes.put(i+"", v);
    	}
		
    	try(BufferedReader br = new BufferedReader(new FileReader(path))) {
    		for(String line; (line = br.readLine()) != null; ) {

    			String[] vertices = line.split("	");
    			String vertex1 = vertices[0];
    			String vertex2 = vertices[1];
    			vertex2 = vertex2.replaceAll(" ", "");
    			
    			Vertex v1 = nodes.get(vertex1);
    			Vertex v2 = nodes.get(vertex2);
    			
    			Edge eLives = orientDBGraph.addEdge(null, v1, v2, "connected");
    		}
    	}
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}

    	

    	//Long total = ((ODocument) odb.query(new OSQLSynchQuery<ODocument>("SELECT COUNT(*) as count FROM Vertex")).get(0)).field("count");
    	//System.out.println("obd: "+ total);
 
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
    
    //Retrieve nodes based on the node ID for 10% of the nodes 
	// in the database that have been previously randomly selected
    long query1OrientDB(ArrayList<String> nodeIds) 
    {

    	String query = "SELECT * from Vertex where ";
    	for(int i = 0; i < nodeIds.size(); i++)
    	{
    		query += "vertexID = "+nodeIds.get(i);
    		if(i != nodeIds.size()-1)
    			query += " or ";
    	}
    	
   
    	OSQLSynchQuery<OrientVertex> qr = new OSQLSynchQuery<OrientVertex>(query);
    
    	long startTime = System.nanoTime();
    	Iterable<OrientVertex> vertices = odb.command(qr).execute();
    	long endTime = System.nanoTime();
    	Iterator<OrientVertex> itr = vertices.iterator();
    	//while(itr.hasNext()) {

    	//	Object element = itr.next(); 
    	 //   System.out.println(element + " ");

    	//} 
    	
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
    }
    
    static long runQuery1(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query1OrientDB(nodeIds);

    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));

    	}
    	return totalTime / numberOfRuns;
    }
    //Retrieve nodes based on the node ID for 10% of the nodes 
    //in the database that have been previously randomly selected, 
    //group by 10% of the number of nodes being retrieved
    long query2OrientDB(ArrayList<String> nodeIds)
    {
    	String query = "SELECT * from Vertex LET $temp = (SELECT * from Vertex where ";
    	for(int i = 0; i < nodeIds.size(); i++)
    	{
    		query += "vertexID = "+nodeIds.get(i);
    		if(i != nodeIds.size()-1)
    			query += " or ";
    	}
    	query += ") GROUP BY Attribute";
    	OSQLSynchQuery<OrientVertex> qr = new OSQLSynchQuery<OrientVertex>(query);

    	long startTime = System.nanoTime();
    	Iterable<OrientVertex> vertices = odb.command(qr).execute();
    	long endTime = System.nanoTime();

    	Iterator<OrientVertex> itr = vertices.iterator();
    	//while(itr.hasNext()) {

    	//	Object element = itr.next(); 
    	  //  System.out.println(element + " ");

    	//} 
    	
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
    
    static long runQuery2(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query2OrientDB(nodeIds);


    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));
    		System.out.println("it");
    	}
    	return totalTime / numberOfRuns;
    }
    
    //Perform Breadth-First Search to level 2 from a given node 
    //for 10% of the nodes in the database that have been previously 
    //randomly selected
	long query3OrientDB(ArrayList<String> nodeIds) 
	{

		String query = "select * from ( traverse * from Vertex WHILE $depth <= 2 strategy BREADTH_FIRST) where ";
    	for(int i = 0; i < nodeIds.size(); i++)
    	{
    		query += "vertexID = "+nodeIds.get(i);
    		if(i != nodeIds.size()-1)
    			query += " or ";
    	}
		OSQLSynchQuery<OrientVertex> qr = new OSQLSynchQuery<OrientVertex>(query);
		long startTime = System.nanoTime();
    	Iterable<OrientVertex> vertices = odb.command(qr).execute();
    	long endTime = System.nanoTime();
    	Iterator<OrientVertex> itr = vertices.iterator();
    	//while(itr.hasNext()) {

    	//	Object element = itr.next(); 
    	   // System.out.println(element + " ");

    	//} 
    	
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
    static long runQuery3(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query3OrientDB(nodeIds);

    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));

    	}
    	return totalTime / numberOfRuns;
    }
	
    //Perform Breadth-First Search to level 4 from a given node 
    //for 10% of the nodes in the database that have been previously 
    //randomly selected
	long query4OrientDB(ArrayList<String> nodeIds) 
	{
		//where $depth == 4"
		String query = "select * from ( traverse * from Vertex WHILE $depth <= 4 strategy BREADTH_FIRST) where ";
    	for(int i = 0; i < nodeIds.size(); i++)
    	{
    		query += "vertexID = "+nodeIds.get(i);
    		if(i != nodeIds.size()-1)
    			query += " or ";
    	}
		OSQLSynchQuery<OrientVertex> qr = new OSQLSynchQuery<OrientVertex>(query);
		long startTime = System.nanoTime();
    	Iterable<OrientVertex> vertices = odb.command(qr).execute();
    	long endTime = System.nanoTime();
    	Iterator<OrientVertex> itr = vertices.iterator();
    	//while(itr.hasNext()) {

    	//	Object element = itr.next(); 
    	 //   System.out.println(element + " ");

    	//} 
    	
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
    static long runQuery4(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query4OrientDB(nodeIds);

    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));

    	}
    	return totalTime / numberOfRuns;
    }

	//Compute Connected Components of the graph using outgoing edges
	long query5OrientDB() 
	{
	 	int currentCluster = 0;
	  	long startTime = System.nanoTime();
		Iterable<Vertex> vertices = orientDBGraph.getVertices();
		clusters = new HashMap<String, Integer>();
		
		for(Vertex v : vertices)
		{
			if(clusters.containsKey(v.getProperty("vertexID")) == false)
			{
				CC(v, currentCluster, Direction.OUT);
				currentCluster++;
			}
		}
		
		
		//System.out.println("Number of centers: " + (currentCluster));
		long endTime = System.nanoTime();

    	
    	long durationInMiliseconds = (endTime - startTime)/1000000; 

    	return durationInMiliseconds;
	}
	
    static long runQuery5(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query5OrientDB();

    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));

    	}
    	return totalTime / numberOfRuns;
    }
	
	//Compute Connected Components of the graph using incoming edges
	long query6OrientDB() 
	{
	 	int currentCluster = 0;
	  	

		long startTime = System.nanoTime();
		Iterable<Vertex> vertices = orientDBGraph.getVertices();
		clusters = new HashMap<String, Integer>();
		
		for(Vertex v : vertices)
		{
			if(clusters.containsKey(v) == false)
			{
				CC(v, currentCluster, Direction.IN);
				currentCluster++;
			}
		}
		
		
		//System.out.println("Number of centers: " + (currentCluster));
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}
	
    static long runQuery6(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query6OrientDB();

    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));

    	}
    	return totalTime / numberOfRuns;
    }
	
	void CC(Vertex v, int currentCluster, Direction dir)
	{
		ArrayList<Vertex> verts = new ArrayList<Vertex>();
		verts.add(v);
		while(verts.size() > 0)
		{
			Vertex currentVert = verts.remove(0);
			Iterable<Vertex> nextVerts = currentVert.getVertices(dir);
			clusters.put(currentVert.getProperty("vertexID"), currentCluster);
			
			for(Vertex nv : nextVerts)
			{
				if(clusters.containsKey(nv.getProperty("vertexID")) == false)
				{
					clusters.put(nv.getProperty("vertexID"), currentCluster);
					verts.add(nv);
				}
					
			}
		}
	}
	
    // Compute Shortest Path for a pair of nodes for 10\% of the nodes in the database that have been previously randomly selected
	long query7OrientDB(ArrayList<String> nodeIds, HashMap<String, Vertex> nodes) 
	{
		long startTime = System.nanoTime();
		for(String ids : nodeIds)
		{
			String[] idPair = ids.split(" ");
			String firstVertex = idPair[0];
			String secondVertex = idPair[1];
			String query = "select shortestpath(" + nodes.get(firstVertex).getId() + ", " + nodes.get(secondVertex).getId() + ").asString()";
			OSQLSynchQuery<OrientVertex> qr = new OSQLSynchQuery<OrientVertex>(query);

			Iterable<OrientVertex> vertices = odb.command(qr).execute();
	    	Iterator<OrientVertex> itr = vertices.iterator();
	    	/*while(itr.hasNext()) {

	    		Object element = itr.next(); 
	    	    System.out.println(element + " ");

	    	} */

		}
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	return durationInMiliseconds;
	}

    static long runQuery7(OrientDBBenchmark myOrientInstance, ArrayList<String> nodeIds)
    {
    	long totalTime = 0;
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		totalTime += myOrientInstance.query7OrientDB(nodeIds, nodes);

    		myOrientInstance.odb.commit();
    		myOrientInstance.orientDBGraph.commit();
    		myOrientInstance.odb.close();
    		myOrientInstance.orientDBGraph.shutdown();
    		myOrientInstance.factory.close();
    		myOrientInstance.factory.drop();
    		removeDirectory(new File(ORIENT_DB_PATH));

    	}
    	return totalTime / numberOfRuns;
    }
	
	//Insert 10\% of the number of nodes in the database into the database
	ArrayList<Long> modifyQuery1And2OrientDB(int numberOfNewNodes, ArrayList<String> connections, int numberOfNodes, String path)
	{
    	factory = new OrientGraphFactory("plocal:"+ORIENT_DB_PATH+"../OrientTmp/");
    	OrientGraphNoTx localGraph = factory.getNoTx();
  	
		HashMap<String, Vertex> nodes = new HashMap<String, Vertex>();
	  	
	  	for(int i = 1; i <= numberOfNodes; i++)
    	{
    		Vertex v = localGraph.addVertex("class:Vertex");
    		v.setProperty("vertexID", i+"");
    		nodes.put(i+"", v);
    	}
		
    	try(BufferedReader br = new BufferedReader(new FileReader(path))) {
    		for(String line; (line = br.readLine()) != null; ) {

    			String[] vertices = line.split("	");
    			String vertex1 = vertices[0];
    			String vertex2 = vertices[1];
    			vertex2 = vertex2.replaceAll(" ", "");
    			
    			Vertex v1 = nodes.get(vertex1);
    			Vertex v2 = nodes.get(vertex2);
    			
    			Edge eLives = localGraph.addEdge(null, v1, v2, "connected");
    		}
    	}
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}
    	
    	ArrayList<Long> times = new ArrayList<Long>();
	  	
    	//Insert Vertices
    	ArrayList<Vertex> newVerts = new ArrayList<Vertex>();
		long startTime = System.nanoTime();
    	for(int i = numberOfNodes+1; i <= numberOfNodes+numberOfNewNodes; i++)
    	{
    		Vertex v = localGraph.addVertex("class:Vertex");
    		v.setProperty("vertexID", i+"");
    		nodes.put(i+"", v);
    		newVerts.add(v);
    	}

    	
		long endTime = System.nanoTime();
    	long durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);

    	//Insert Edges
    	startTime = System.nanoTime();
    	ArrayList<Edge> edges = new ArrayList<Edge>();
		for(String line : connections ) 
		{

			String[] vertices = line.split(" ");
			String vertex1 = vertices[0];
			String vertex2 = vertices[1];
			vertex2 = vertex2.replaceAll(" ", "");

			Vertex v1 = nodes.get(vertex1);
			Vertex v2 = nodes.get(vertex2);

			Edge eLives = localGraph.addEdge(null, v1, v2, "connected");
			edges.add(eLives);
		}

		endTime = System.nanoTime();
    	durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);
    
    	
    	//Delete edges
    	startTime = System.nanoTime();
    	
    	for(int i = 0; i < edges.size(); i++)
    	{
    		localGraph.removeEdge(edges.get(i));
    	}
    	endTime = System.nanoTime();
    	durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);
    
    	
    	//Delete vertices
    	startTime = System.nanoTime();
    	
    	for(int i = 0; i < newVerts.size(); i++)
    	{
    		localGraph.removeVertex(newVerts.get(i));
    	}
    	endTime = System.nanoTime();
    	durationInMiliseconds = (endTime - startTime)/1000000; 
    	times.add(durationInMiliseconds);
    	
    	localGraph.commit();
    	localGraph.shutdown();
		factory.close();
		factory.drop();
    	
    	return times;
	}
	
    static ArrayList<Long> runQuery8(OrientDBBenchmark myOrientInstance, ArrayList<String> newNodeConnections)
    {
    	ArrayList<Long> totalTime = new ArrayList<Long>();
    	totalTime.add((long) 0);
    	totalTime.add((long) 0);
    	totalTime.add((long) 0);
    	totalTime.add((long) 0);
    	for(int x = 0; x < numberOfRuns; x++)
    	{
            //HashMap<String, Vertex> nodes = myOrientInstance.CreateOrientDBWithData(ORIENT_DB_PATH,numberOfNodes,
            //		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		ArrayList<Long> newTimes = myOrientInstance.modifyQuery1And2OrientDB(newNodeConnections.size(), newNodeConnections, numberOfNodes,
            		"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
    		
    		for(int i = 0; i < newTimes.size(); i++)
    		{
    			totalTime.set(i, totalTime.get(i)+newTimes.get(i));
    		}

    		//myOrientInstance.odb.commit();
    		//myOrientInstance.orientDBGraph.commit();
    		//myOrientInstance.odb.close();
    		//myOrientInstance.orientDBGraph.shutdown();
    		removeDirectory(new File(ORIENT_DB_PATH));
    		removeDirectory(new File("./OrientTmp/"));

    	}
		for(int i = 0; i < totalTime.size(); i++)
		{
			totalTime.set(i, totalTime.get(i)/numberOfRuns);
		}
    	
    	return totalTime;
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
	
    void shutDown()
    {
        System.out.println("graphDB shut down.");   
    }   
}

