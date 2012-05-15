package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.TableStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Table {

	private String name;
	private KeySchema keySchema;
	private ProvisionedThroughput provisionedThroughput;
	private List<String> attributes;
	private List<Item> items;
	private Date creationDate;
	private final TableStatus status = TableStatus.ACTIVE;; // Set active right away since we don't need to wait

	public Table(String name, KeySchema keySchema, ProvisionedThroughput provisionedThroughput) {
		this.name = name;
		this.keySchema = keySchema;
		this.provisionedThroughput = provisionedThroughput;
		this.items = new ArrayList<Item>();
		this.creationDate = new Date();
	}

	public List<Item> getItemsWithKey(String hashKey) {
		List<Item> res = new ArrayList<Item>();
		for (Item item : this.items) {
			if (hashKey.equals(item.getAttributes().get(item.getHashKey()))) {
				res.add(item);
			}
		}
		return res;
	}

	public void addItem(Item item) {
		items.add(item);
	}

	public void removeItem(Item item) {
		items.remove(item);
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<String> attributes) {
		this.attributes = attributes;
	}

	public KeySchema getKeySchema() {
		return keySchema;
	}

	public ProvisionedThroughput getProvisionedThroughput() {
		return provisionedThroughput;
	}

	public Long getItemCount() {
		return new Long(items.size());
	}

	public Long getSizeBytes() {
		return new Long(items.size());
	}

	public List<Item> getItems() {
		return items;
	}

	public void setItems(List<Item> items) {
		this.items = items;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public TableStatus getStatus() {
		return status;
	}
}
