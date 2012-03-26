/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.db;

import java.util.HashMap;

/**
 *
 * @author thomasbredillet
 */
public class Element {
    private Table table;
    private String hashKey;
    private String rangeKey;
    private HashMap<String,String> attributes;
    
    
    public Element(Table table,String hashKey,String rangeKey,HashMap<String,String> map){
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.table = table;
        this.attributes = map;
    }

    /**
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(Table table) {
        this.table = table;
    }

    /**
     * @return the attributes
     */
    public HashMap<String,String> getAttributes() {
        return attributes;
    }

    /**
     * @param attributes the attributes to set
     */
    public void setAttributes(HashMap<String,String> attributes) {
        this.attributes = attributes;
    }

    /**
     * @return the hashKey
     */
    public String getHashKey() {
        return hashKey;
    }

    /**
     * @param hashKey the hashKey to set
     */
    public void setHashKey(String hashKey) {
        this.hashKey = hashKey;
    }

    /**
     * @return the rangeKey
     */
    public String getRangeKey() {
        return rangeKey;
    }

    /**
     * @param rangeKey the rangeKey to set
     */
    public void setRangeKey(String rangeKey) {
        this.rangeKey = rangeKey;
    }
}
