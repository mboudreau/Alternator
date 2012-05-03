package com.michelboudreau.alternator.models;

import java.util.HashMap;
import java.util.Map;

public class Item {
	private String tableName;
	private String hashKey;
	private String rangeKey;
	boolean hasRangeKey;
	private HashMap<String, Map<String, String>> attributes;

	public Item() {

	}

	public Item(String tableName, String hashKey, String rangeKey, HashMap<String, Map<String, String>> map) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		this.tableName = tableName;
		this.attributes = map;
	}

	/**
	 * @return the table
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param table the table to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the attributes
	 */
	public HashMap<String, Map<String, String>> getAttributes() {
		return attributes;
	}

	/**
	 * @param attributes the attributes to set
	 */
	public void setAttributes(HashMap<String, Map<String, String>> attributes) {
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
