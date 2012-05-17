package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.*;

import java.util.*;

public class Table {

	private String name;
	private KeySchema keySchema;
	private ProvisionedThroughputDescription throughputDescription;
	private Date lastDecreaseDateTime;
	private Date lastIncreaseDateTime;
	private Map<String, Item> items = new HashMap<String, Item>();
	private List<Item> itemList;
	private Date creationDate;
	private String hashKeyName;
	private String rangeKeyName;
	private final TableStatus status = TableStatus.ACTIVE; // Set active right away since we don't need to wait

	public Table(String name, KeySchema keySchema, ProvisionedThroughput throughput) {
		this.name = name;
		this.keySchema = keySchema;
		setProvisionedThroughput(throughput);
		this.itemList = new ArrayList<Item>();
		this.creationDate = new Date();
		this.lastDecreaseDateTime = new Date();
		this.lastIncreaseDateTime = new Date();

		// Get hash key name
		hashKeyName = keySchema.getHashKeyElement().getAttributeName();
		if (keySchema.getRangeKeyElement() != null) {
			rangeKeyName = keySchema.getRangeKeyElement().getAttributeName();
		}
	}

	public void putItem(Item item) {
		String keyValue = getHashKeyValue(item);
		//TODO: add exception if null or empty
		if (keyValue != null) {
			items.put(keyValue, item);
			itemList.add(item);
		}
	}

	public void removeItem(Item item) {
		String keyValue = getHashKeyValue(item);
		//TODO: add exception if null or empty
		if (keyValue != null) {
			itemList.remove(item);
			items.remove(keyValue);
		}
	}

	public Item getItem(String hashKey) {
		return items.get(hashKey);
	}

	public Map<String, Item> getItems() {
		return items;
	}

	public List<Item> getItemList() {
		return itemList;
	}

	public KeySchema getKeySchema() {
		return keySchema;
	}

	public String getHashKeyName() {
		return hashKeyName;
	}

	public String getRangeKeyName() {
		return rangeKeyName;
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
				lastIncreaseDateTime = new Date();
			}
			if (throughput.getReadCapacityUnits() > oldThroughput.getReadCapacityUnits() || throughput.getWriteCapacityUnits() > oldThroughput.getWriteCapacityUnits()) {
				lastDecreaseDateTime = new Date();
			}
		}
		desc.setLastIncreaseDateTime(lastIncreaseDateTime);
		desc.setLastDecreaseDateTime(lastDecreaseDateTime);
		throughputDescription = desc;
	}

	public Long getItemCount() {
		return new Long(itemList.size());
	}

	public Long getSizeBytes() {
		return new Long(itemList.size());
	}

	public String getName() {
		return name;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public TableStatus getStatus() {
		return status;
	}

	public Date getLastDecreaseDateTime() {
		return lastDecreaseDateTime;
	}

	public Date getLastIncreaseDateTime() {
		return lastIncreaseDateTime;
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

	protected String getHashKeyValue(Item item) {
		AttributeValue value = item.getAttributes().get(getHashKeyName());
		if (value.getN() != null) {
			return value.getN();
		} else if (value.getS() != null) {
			return value.getS();
		}
		return null;
	}
}
