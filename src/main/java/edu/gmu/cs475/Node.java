package edu.gmu.cs475;
import java.util.*;

public class Node{

  private HashMap<String, String> keyToValue = new HashMap <>();
  private String id;

  public Node(String id){
    this.id = id;
  }

  public void setValue(String key, String value){
    
    this.keyToValue.put(key, value);
  }

  public String getValue(String key){
	  
  //  for(KeyObj k: this.keyToValue.keySet()){
    //  if(k.getKey().equals(k)){
        return this.keyToValue.get(key);
   // }
  }
  

  public void remove(String key){
  //   for(KeyObj k: this.keyToValue.keySet()){
    //  if(k.getKey().equals(k)){
        this.keyToValue.remove(key);
     // }
     //}  
  }

}
