/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.db;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author thomasbredillet
 */
public class Table {

    private String hashKey;
    private String rangeKey;
    private boolean hasRangeKey;
    private List<String> attributes;
    private List<Item> items;
    private String name;

    public Table(){
        
    }
    public Table(String hashKey, String rangeKey, String name){
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.name = name;
        this.items = new ArrayList<Item>();
    }
    public void addItem(Item item){
        items.add(item);
    }
    public void removeItem(Item item) {
        items.remove(item);
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

    /**
     * @return the hasRangeKey
     */
    public boolean isHasRangeKey() {
        return hasRangeKey;
    }

    /**
     * @param hasRangeKey the hasRangeKey to set
     */
    public void setHasRangeKey(boolean hasRangeKey) {
        this.hasRangeKey = hasRangeKey;
    }

    /**
     * @return the attributes
     */
    public List<String> getAttributes() {
        return attributes;
    }

    /**
     * @param attributes the attributes to set
     */
    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    /**
     * @return the items
     */
    public List<Item> getItems() {
        return items;
    }

    /**
     * @param items the items to set
     */
    public void setItems(List<Item> items) {
        this.items = items;
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
}
