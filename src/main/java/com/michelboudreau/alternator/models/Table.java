package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Table {

	private String name;
	private KeySchema keySchema;
	private ProvisionedThroughputDescription throughputDescription;
	private Date lastDecreaseDateTime;
	private Date lastIncreaseDateTime;
	private List<String> attributes;
	private List<Item> items;
	private Date creationDate;
	private final TableStatus status = TableStatus.ACTIVE;
	; // Set active right away since we don't need to wait

	public Table(String name, KeySchema keySchema, ProvisionedThroughput throughput) {
		this.name = name;
		this.keySchema = keySchema;
		setProvisionedThroughput(throughput);
		this.items = new ArrayList<Item>();
		this.creationDate = new Date();
		this.lastDecreaseDateTime = new Date();
		this.lastIncreaseDateTime = new Date();
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

	public ProvisionedThroughputDescription getProvisionedThroughputDescription() {
		return throughputDescription;
	}

	public void setProvisionedThroughput(ProvisionedThroughput throughput) {
		ProvisionedThroughputDescription desc = new ProvisionedThroughputDescription();
		desc.setReadCapacityUnits(throughput.getReadCapacityUnits());
		desc.setWriteCapacityUnits(throughput.getWriteCapacityUnits());
		ProvisionedThroughputDescription oldThroughput = getProvisionedThroughputDescription();
		if (oldThroughput != null) {
			if (throughput.getReadCapacityUnits() > oldThroughput.getReadCapacityUnits() || throughput.getWriteCapacityUnits() > oldThroughput.getWriteCapacityUnits()) {
				desc.setLastIncreaseDateTime(new Date());
			}
			if (throughput.getReadCapacityUnits() > oldThroughput.getReadCapacityUnits() || throughput.getWriteCapacityUnits() > oldThroughput.getWriteCapacityUnits()) {
				desc.setLastDecreaseDateTime(new Date());
			}
		}
		throughputDescription = desc;
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

	public TableDescription getTableDescription() {
		TableDescription desc = new TableDescription();
		desc.setTableName(getName());
		desc.setTableStatus(getStatus());
		desc.setItemCount(getItemCount());
		desc.setTableSizeBytes(getSizeBytes());
		desc.setCreationDateTime(getCreationDate());
		desc.setKeySchema(getKeySchema());
		desc.setProvisionedThroughput(getProvisionedThroughputDescription());
		return desc;
	}
}
