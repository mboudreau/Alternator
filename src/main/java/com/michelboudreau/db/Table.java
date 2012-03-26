/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author thomasbredillet
 */
public class Table {
    private String name;
    private String hashKeyName;
    private String rangeKeyName;
    private String[] attributesNames;
    private List<Element> elements;
    
  public Table(String name,String hashKeyName,String rangeKeyName,String[] attributesNames) {
      this.rangeKeyName = rangeKeyName;
      this.hashKeyName = hashKeyName;
      this.attributesNames = attributesNames;
      this.name = name;
      elements = new ArrayList<Element>();
  } 
  
   public Table(String name,String hashKeyName,String[] attributesNames) {
      rangeKeyName = null;
      this.hashKeyName = hashKeyName;
      this.attributesNames = attributesNames;
      this.name = name;
  }
   
  public void addElement(String hashKey, String rangeKey,HashMap<String,String> map) {
      Element element = new Element(this,hashKey,rangeKey,map);
        getElements().add(element);
  }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the hashKeyName
     */
    public String getHashKeyName() {
        return hashKeyName;
    }

    /**
     * @param hashKeyName the hashKeyName to set
     */
    public void setHashKeyName(String hashKeyName) {
        this.hashKeyName = hashKeyName;
    }

    /**
     * @return the rangeKeyName
     */
    public String getRangeKeyName() {
        return rangeKeyName;
    }

    /**
     * @param rangeKeyName the rangeKeyName to set
     */
    public void setRangeKeyName(String rangeKeyName) {
        this.rangeKeyName = rangeKeyName;
    }

    /**
     * @return the attributesNames
     */
    public String[] getAttributesNames() {
        return attributesNames;
    }

    /**
     * @param attributesNames the attributesNames to set
     */
    public void setAttributesNames(String[] attributesNames) {
        this.attributesNames = attributesNames;
    }

    /**
     * @return the elements
     */
    public List<Element> getElements() {
        return elements;
    }

    /**
     * @param elements the elements to set
     */
    public void setElements(List<Element> elements) {
        this.elements = elements;
    }
          
}
