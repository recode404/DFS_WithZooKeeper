package edu.gmu.cs475;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;

import edu.gmu.cs475.internal.IKVStore;
import sun.reflect.generics.tree.Tree;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;

public class KVStore extends AbstractKVStore {
	// Node object that holds all keys and values
	public Node node;
  private HashMap <String, KeyObj> lockMap = new HashMap<>();
	// this is for the leader to map nodes to their unique id's
	private HashMap<String, Node> nodeTracker = new HashMap<>();
	// keeps track of this Node's connection state, updated when the
	// stateChanged call back is called
	// private ConnectionState connectionState = null;
	// object representing this node
	private boolean isConnected = true;
	// private PersistentNode myMembership = null;
	private LeaderLatch leader;
	private ArrayList<String> nodeArr;

	/*
	 * Do not change these constructors. Any code that you need to run when the
	 * client starts should go in initClient.
	 */
	public KVStore(String zkConnectString) {
		super(zkConnectString);
	}

	public KVStore(String zkConnectString, int i1, int i2, int i3) {
		super(zkConnectString, i1, i2, i3);
	}

	/**
	 * This callback is invoked once your client has started up and published an RMI
	 * endpoint.
	 * <p>
	 * In this callback, you will need to set-up your ZooKeeper connections, and
	 * then publish your RMI endpoint into ZooKeeper (publishing the hostname and
	 * port)
	 * <p>
	 * You will also need to set up any listeners to track ZooKeeper events
	 *
	 * @param localClientHostname
	 *            Your client's hostname, which other clients will use to contact
	 *            you
	 * @param localClientPort
	 *            Your client's port number, which other clients will use to contact
	 *            you
	 */
	@Override
	public void initClient(String localClientHostname, int localClientPort) {
		this.nodeArr = new ArrayList<>();
		TreeCache myTreeCache = new TreeCache(zk, ZK_MEMBERSHIP_NODE);
		this.node = new Node(getLocalConnectString());
		PersistentNode myMembership = new PersistentNode(zk, CreateMode.EPHEMERAL, false,
		ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString(), new byte[0]);
		myMembership.start();

		// System.out.println("init");
		this.leader = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());
		try {
			myTreeCache.start();

		} catch (Exception e) {

		}

		try {
			this.leader.start();
		} catch (Exception e) {
			// e.printStackTrace();

		}

		isConnected = true;
	}

	/**
	 * Retrieve the value of a key
	 *
	 * @param key
	 * @return The value of the key or null if there is no such key
	 * @throws IOException
	 *             if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public String getValue(String key) throws IOException {
		
		IKVStore rmiLeader = null;
		String wtf = this.node.getValue(key);
		System.out.println("isConnected: " + this.isConnected);
    if(!this.isConnected){ 
      System.out.println("IOEXCP");
      throw new IOException(); 
    
    }

		//System.out.println(wtf);
			
		if (wtf == null) {
			
			String ldr = null;

			try {

				ldr = this.leader.getLeader().getId();
				
        String ldrValue = super.connectToKVStore(ldr).getValue(key,getLocalConnectString()); 

				if (ldrValue != null) {
					
          this.node.setValue(key, ldrValue);
					
					wtf = this.node.getValue(key);
				}
        
			} catch (Exception e) {
       
        e.printStackTrace();
			}
		}
		
		return wtf;
	}

	/**
	 * Update the value of a key. After updating the value, this new value will be
	 * locally cached.
	 *
	 * @param key
	 * @param value
	 * @throws IOException
	 *             if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public void setValue(String key, String value) throws IOException {
		
    if(!this.isConnected){ throw new IOException(); }
		String ldr = null;
		try {
			
			ldr = this.leader.getLeader().getId();	
			IKVStore rmiLeader = super.connectToKVStore(ldr);
			//synchronized(((KVStore)rmiLeader).getKeyObject(key)){
      String clientString = super.getLocalConnectString();	
			KeyObj k = this.getKeyObject(key);
      /*if(k == null){
        this.lockMap.put(key,new KeyObj(key));
        k = this.lockMap.get(key);
      }*/
      synchronized(k){
      rmiLeader.setValue(key, value, clientString);
		  this.node.setValue(key, value);
      }
     // }
    } catch (Exception e) {
			e.printStackTrace();
		}

	}

  public KeyObj getKeyObject(String key){
    KeyObj k = this.lockMap.get(key);
    if(this.lockMap.get(key)== null){
      k = new KeyObj(key);
      this.lockMap.put(key, k);
    }
    return k;
  }

  public String getLeaderName(){

    String ldr = null;
		try {
			// System.out.println("Leader is: " + this.leader);
			ldr = this.leader.getLeader().getId();
    }
    catch(Exception e){ }
    return ldr;
  }

	/**
	 * Request the value of a key. The node requesting this value is expected to
	 * cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 *
	 * @param key
	 *            The key requested
	 * @param fromID
	 *            The ID of the client making the request (as returned by
	 *            AbstractKVStore.getLocalConnectString())
	 * @return The value of the key, or null if there is no value for this key
	 *         <p>
	 *         DOES NOT throw any exceptions (the RemoteException is thrown by RMI
	 *         if the connection fails)
	 */
	@Override
	public String getValue(String key, String fromID) throws RemoteException {
    //if(!this.isConnected){ throw new IOException(); }
		if(!this.nodeArr.contains(fromID) && !super.getLocalConnectString().equals(fromID)){
      this.nodeArr.add(fromID);
    }
    return this.node.getValue(key);

	}

	/**
	 * Request that the value of a key is updated. The node requesting this update
	 * is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 * <p>
	 * This command must wait for any pending writes on the same key to be completed
	 *
	 * @param key
	 *            The key to update
	 * @param value
	 *            The new value
	 * @param fromID
	 *            The ID of the client making the request (as returned by
	 *            AbstractKVStore.getLocalConnectString())
	 */
	@Override
	public void setValue(String key, String value, String fromID) throws IOException {
    if(!this.isConnected){ throw new IOException(); }
    KeyObj k = this.lockMap.get(key);
		if (!this.nodeArr.contains(fromID) && !super.getLocalConnectString().equals(fromID)) {
			// this.nodeTracker.put(fromID, new Node(fromID));
			this.nodeArr.add(fromID);
		}
		if(this.node.getValue(key) != null){
      //System.out.println("key is not null");
      for (String id : this.nodeArr) {
			  IKVStore tmp = this.getRMIObject(id);
			  //System.out.println("invalidate loop " + id);
        tmp.invalidateKey(key);
		  }
      
      //this.invalidateKey(key);
    
    }
    
    else{
      k = new KeyObj(key);
      synchronized(this.lockMap){
        this.lockMap.put(key, k);
      }
    
    }
    
    //synchronized(k){
      this.node.setValue(key , value);
    //}
		// Node rep = this.nodeTracker.get(fromID);
		// rep.setValue(key, value);
		// this.setValue(key, value);
	}

 
	/**
	 * Instruct a node to invalidate any cache of the specified key.
	 * <p>
	 * This method is called BY the LEADER, targeting each of the clients that has
	 * cached this key.
	 *
	 * @param key
	 *            key to invalidate
	 *            <p>
	 *            DOES NOT throw any exceptions (the RemoteException is thrown by
	 *            RMI if the connection fails)
	 */

  public IKVStore getRMIObject(String id){
    IKVStore rmi = null;
    try{
      rmi = super.connectToKVStore(id);
    } catch(Exception e){ }
    return rmi;
  }

	@Override
	public void invalidateKey(String key) throws RemoteException {
		
    /*if(!super.getLocalConnectString().equals(this.getLeaderName())){
      this.node.remove(key);
      //System.out.println("Client Invalidate : " + this.getLocalConnectString());
    }
    else{
       
      for (String id : this.nodeArr) {
			  IKVStore tmp = this.getRMIObject(id);
			  //System.out.println("invalidate loop " + id);
        tmp.invalidateKey(key);
		  }
    }*/
    this.node.remove(key);
	}

	/**
	 * Called when ZooKeeper detects that your connection status changes
	 * 
	 * @param curatorFramework
	 * @param connectionState
	 */
	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		//this.zk.close();
    //this.zk.start();
		this.isConnected = newState.isConnected();
	   
    if(newState == ConnectionState.valueOf("CONNECTED")){
      System.out.println("Connectedddddddddddddd");
    }
    else if(newState == ConnectionState.valueOf("LOST")){
      System.out.println("Losttttttttttttttttt");
    }
    else if(newState == ConnectionState.valueOf("READ_ONLY")){
      System.out.println("Read Onlyyyyyyyyyyyyyyyy");
    }
    else if(newState == ConnectionState.valueOf("RECONNECTED")){
      System.out.println("RRRRRRRREConnecteddddddddddddddddd");
    }
    else if(newState == ConnectionState.valueOf("SUSPENDED")){
      System.out.println("Suspendedddddddddddddddd");
    }
    System.out.println("This objects zookeeper client connection string: " + this.zk.getZookeeperClient().getCurrentConnectionString() + " This object is: " + super.getLocalConnectString() + " and the client param is: " + client.getZookeeperClient().getCurrentConnectionString() + " the new state is: " + newState.isConnected() + " and the leader is: " + this.getLeaderName());
    
  
  }

	/**
	 * Release any ZooKeeper resources that you setup here (The connection to
	 * ZooKeeper itself is automatically cleaned up for you)
	 */
	@Override
	protected void _cleanup() {

	}
}
