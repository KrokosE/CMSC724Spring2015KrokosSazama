import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.configuration.BaseConfiguration;

import com.google.common.base.Predicates;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanIndexQuery.Result;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.Text;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;


public class TitanBenchmark {

	TitanGraph titanGraph;
	private static final String TITAN_DB_PATH = "./TitanData";

    static int numberOfNodes = 50000;
    static String fileSet = "N50000k50m100";
    static int numberOfRuns = 4;
    
    HashMap<String, Integer> clusters;

	public static void main(String[] args)
	{
		TitanBenchmark titan = new TitanBenchmark();
		
        PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream("output.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.setOut(out);
		
		ArrayList<String> nodeIds = CreateNodeIds(0.1f, numberOfNodes);
		System.out.println("Query1: " + runQuery1(titan,nodeIds)+"ms");
		System.out.println("Query2: " + runQuery2(titan,nodeIds)+"ms");
		System.out.println("Query3: " + runQuery3(titan,nodeIds)+"ms");
		System.out.println("Query4: " + runQuery4(titan,nodeIds)+"ms");
		System.out.println("Query5: " + runQuery5(titan)+"ms");
		System.out.println("Query6: " + runQuery6(titan)+"ms");


		ArrayList<String> nodePairs = GetSomePairs(0.1f, numberOfNodes);
		System.out.println("Query7: " + runQuery7(titan, nodePairs)+"ms");

		ArrayList<String> newNodeConnections = GetSomeNewerPairs(0.1f, numberOfNodes);
		System.out.println("Query8: " + runQuery8(titan, newNodeConnections)+"ms");
		 
	}

	static ArrayList<String> CreateNodeIds(float percent, int numberOfNodes)
	{
		int numberToCreate = (int) (percent*numberOfNodes);
		ArrayList<String> nodes = new ArrayList<String>();
		Random rand = new Random();
		for(int i = 0; i < numberToCreate; i++)
		{
			nodes.add((rand.nextInt(numberOfNodes) + 1)+"");
		}
		return nodes;
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

	HashMap<String,Vertex> CreateTitanWithData(String databasePath,int numberOfNodes, String path)
	{
		BaseConfiguration conf = new BaseConfiguration();
		conf.setProperty("storage.directory", databasePath);
		conf.setProperty("storage.backend", "berkeleyje");
		titanGraph = TitanFactory.open(conf);
		HashMap<String, Vertex> nodes = new HashMap<String, Vertex>();
		ArrayList<Vertex> vertices = new ArrayList<Vertex>();
		TitanKey nameKey = titanGraph.makeKey("vertexID").dataType(String.class).make();
		TitanKey key = titanGraph.makeKey("attribute1").dataType(Integer.class).make();

		titanGraph.commit();
		for(int i = 1; i <= numberOfNodes; i++)
		{
			Vertex v = titanGraph.addVertex(null);
			v.setProperty("vertexID", i+"");
			v.setProperty("attribute1", new Integer(i%4));
			nodes.put(i+"", v);
			vertices.add(v);
		}

		try(BufferedReader br = new BufferedReader(new FileReader(path))) {
			for(String line; (line = br.readLine()) != null; ) {

				String[] verts = line.split("	");
				String vertex1 = verts[0];
				String vertex2 = verts[1];
				vertex2 = vertex2.replaceAll(" ", "");


				Vertex v1 = nodes.get(vertex1);
				Vertex v2 = nodes.get(vertex2);
				Edge relationship = titanGraph.addEdge(null, v1, v2, "Connected");

			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}

		titanGraph.commit();

		return nodes;
	}


	//Retrieve nodes based on the node ID for 10% of the nodes 
	// in the database that have been previously randomly selected
	long query1Titan(ArrayList<String> nodeIds) 
	{
		long startTime = System.nanoTime();
		//Iterable<Vertex> verts2 = titanGraph.query().has("vertexID", Text.CONTAINS_REGEX, "1").vertices();

		//Iterable<Vertex> verts = titanGraph.query().interval("vertexID", "1", "100").vertices();
		for(String id : nodeIds)
		{
			Iterable<Vertex> verts = titanGraph.query().has("vertexID", id).vertices();
			/*for(Vertex v : verts)
    		{
    			System.out.println(v.getId());
    		}*/
		}


		long endTime = System.nanoTime();
		long durationInMiliseconds = (endTime - startTime)/1000000; 
		return durationInMiliseconds;
	}
	
	static long runQuery1(TitanBenchmark titan, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query1Titan(nodeIds);
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
		}
		
		return totalTime / numberOfRuns;
	}

	//Retrieve nodes based on the node ID for 10% of the nodes 
	//in the database that have been previously randomly selected, 
	//group by 10% of the number of nodes being retrieved
	long query2Titan(ArrayList<String> nodeIds) 
	{
		long startTime = System.nanoTime();

		//Iterable<Vertex> verts = titanGraph.query().orderBy("attribute1", Order.DESC).vertices();
		//for(String id : nodeIds)
		//{

		//.has.vertices();
		/*for(Vertex v : verts)
    		{
    			System.out.println(v.getId());
    		}*/
		//}
		long endTime = System.nanoTime();
		long durationInMiliseconds = (endTime - startTime)/1000000; 
		return durationInMiliseconds;
	}

	static long runQuery2(TitanBenchmark titan, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query2Titan(nodeIds);
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
		}
		
		return totalTime / numberOfRuns;
	}
	//Perform Breadth-First Search to level 2 from a given node 
	//for 10% of the nodes in the database that have been previously 
	//randomly selected
	long query3Titan(ArrayList<String> nodeIds, HashMap<String,Vertex> nodes)
	{


		long startTime = System.nanoTime();
		for(String node : nodeIds)
		{

			ArrayList<String> allNodes = breadthFirstSearch(nodes.get(node), 0, 2);
		}
		long endTime = System.nanoTime();
		long durationInMiliseconds = (endTime - startTime)/1000000; 
		return durationInMiliseconds;
	}

	static long runQuery3(TitanBenchmark titan, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query3Titan(nodeIds,nodes);
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
		}
		
		return totalTime / numberOfRuns;
	}
	
	//Perform Breadth-First Search to level 4 from a given node 
	//for 10% of the nodes in the database that have been previously 
	//randomly selected
	long query4Titan(ArrayList<String> nodeIds, HashMap<String,Vertex> nodes)
	{
		long startTime = System.nanoTime();
		for(String node : nodeIds)
		{

			ArrayList<String> allNodes = breadthFirstSearch(nodes.get(node), 0, 4);
		}
		long endTime = System.nanoTime();
		long durationInMiliseconds = (endTime - startTime)/1000000; 
		return durationInMiliseconds;
	}

	static long runQuery4(TitanBenchmark titan, ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query4Titan(nodeIds,nodes);
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
		}
		
		return totalTime / numberOfRuns;
	}
	
	ArrayList<String> breadthFirstSearch(Vertex node, int level, int maxLevels)
	{
		ArrayList<Vertex> queue = new ArrayList<Vertex>();
		ArrayList<Vertex> nextSet = new ArrayList<Vertex>();
		while(level < maxLevels)
		{
			queue.add(node);
			while(queue.size() > 0)
			{
				Vertex currNode = queue.remove(0);
				Iterable<Vertex> connected = currNode.getVertices(Direction.OUT);
				for(Vertex conn : connected)
				{
					nextSet.add(conn);
				}
			}

			level++;
			queue = new ArrayList<Vertex>(nextSet);
			nextSet.clear();
		}

		return null;

	}

	//Compute Connected Components of the graph using outgoing edges
	long query5Titan()
	{
		int currentCluster = 0;
		//GremlinPipeline<Vertex,?> pipe = new GremlinPipeline<Vertex,Vertex>(v1).outE("connected").gather().scatter().inV().gather().scatter().inE("connected").gather().scatter().outV().gather().scatter();
		long startTime = System.nanoTime();
		clusters = new HashMap<String, Integer>();
		Iterable<Vertex> vertices = titanGraph.getVertices();

		for(Vertex v : vertices)
		{
			if(clusters.containsKey(v) == false)
			{
				CC(v,currentCluster, Direction.OUT);
				currentCluster++;
			}
		}


		//System.out.println("Number of centers: " + (currentCluster));
		long endTime = System.nanoTime();
		long durationInMiliseconds = (endTime - startTime)/1000000; 
		return durationInMiliseconds;

	}
	
	static long runQuery5(TitanBenchmark titan)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query5Titan();
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
		}
		
		return totalTime / numberOfRuns;
	}

	//Compute Connected Components of the graph using incoming edges
	long query6Titan()
	{
		int currentCluster = 0;
		//GremlinPipeline<Vertex,?> pipe = new GremlinPipeline<Vertex,Vertex>(v1).outE("connected").gather().scatter().inV().gather().scatter().inE("connected").gather().scatter().outV().gather().scatter();
		long startTime = System.nanoTime();
		clusters = new HashMap<String, Integer>();
		Iterable<Vertex> vertices = titanGraph.getVertices();

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
	
	static long runQuery6(TitanBenchmark titan)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query6Titan();
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
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
	long query7Titan(ArrayList<String> nodeIds, HashMap<String, Vertex> nodes) 
	{
		long startTime = System.nanoTime();
		for(String ids : nodeIds)
		{
			String[] idPair = ids.split(" ");
			String firstVertex = idPair[0];
			String secondVertex = idPair[1];

			Vertex v1 = nodes.get(firstVertex);
			Vertex v2 = nodes.get(secondVertex);

			GremlinPipeline pipe = new GremlinPipeline(v1).as("person").both("Connected").loop("person",new PipeFunction<LoopBundle<Vertex>, Boolean>() {
				@Override
				public Boolean compute(LoopBundle<Vertex> bundle) {
					return bundle.getLoops() < 5 && bundle.getObject() != v2;
				}
			}).path();

			if (pipe.hasNext()) {
				final ArrayList<Vertex> shortestPath = (ArrayList<Vertex>) pipe.next();
				for (final Vertex v : shortestPath) {
					//System.out.print(" -> " + v.getProperty("vertexID"));
				}
				//System.out.println();
			}

		}
		long endTime = System.nanoTime();
		long durationInMiliseconds = (endTime - startTime)/1000000; 
		return durationInMiliseconds;
	}
	
	static long runQuery7(TitanBenchmark titan,ArrayList<String> nodeIds)
	{
		long totalTime = 0;
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			totalTime += titan.query7Titan(nodeIds,nodes);
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH));
		}
		
		return totalTime / numberOfRuns;
	}

	//Insert 10\% of the number of nodes in the database into the database
	ArrayList<Long> modifyQuery1And2Titan(int numberOfNewNodes, ArrayList<String> connections, int numberOfNodes, String path)
	{

		TitanGraph localGraph = TitanFactory.open(TITAN_DB_PATH+"/../TitanTmp/");
		TitanKey nameKey = localGraph.makeKey("vertexID").dataType(String.class).make();
		TitanKey key = localGraph.makeKey("attribute1").dataType(Integer.class).make();

		titanGraph.commit();
		HashMap<String, Vertex> nodes = new HashMap<String, Vertex>();

		for(int i = 1; i <= numberOfNodes; i++)
		{
			Vertex v = titanGraph.addVertex(null);
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

				Edge eLives = titanGraph.addEdge(null, v1, v2, "Connected");
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

			Edge eLives = localGraph.addEdge(null, v1, v2, "Connected");
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

		return times;
	}
	
	static ArrayList<Long> runQuery8(TitanBenchmark titan,ArrayList<String> newNodeConnectionss)
	{
		ArrayList<Long> totalTime = new ArrayList<Long>();
		totalTime.add((long) 0);
		totalTime.add((long) 0);
		totalTime.add((long) 0);
		totalTime.add((long) 0);
		
		for(int x = 0; x < numberOfRuns; x++)
		{
			HashMap<String,Vertex> nodes = titan.CreateTitanWithData(TITAN_DB_PATH, numberOfNodes, 
					"./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			ArrayList<Long> newTimes = titan.modifyQuery1And2Titan(newNodeConnectionss.size(), newNodeConnectionss,
					numberOfNodes, "./DirectedDataSets/DirectedDataSets/"+fileSet+"/network.dat");
			
			for(int i = 0; i < newTimes.size(); i++)
			{
				totalTime.set(i, totalTime.get(i)+newTimes.get(i));
			}
			
			titan.titanGraph.shutdown();
			removeDirectory(new File(TITAN_DB_PATH+"/../TitanTmp/"));
			removeDirectory(new File(TITAN_DB_PATH));
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

}
