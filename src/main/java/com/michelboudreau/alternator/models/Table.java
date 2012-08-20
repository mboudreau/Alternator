package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Table {

	private String name;
	private KeySchema keySchema;
	private ProvisionedThroughputDescription throughputDescription;
	private Date lastDecreaseDateTime;
	private Date lastIncreaseDateTime;
    private Map<String, Map<String, AttributeValue>> items = new HashMap<String, Map<String, AttributeValue>>();
	private Date creationDate;
	private String hashKeyName;
	private String rangeKeyName;
	private final TableStatus status = TableStatus.ACTIVE; // Set active right away since we don't need to wait

    private Table(){
    }

	public Table(String name, KeySchema keySchema, ProvisionedThroughput throughput) {
		this.name = name;
		this.keySchema = keySchema;
		setProvisionedThroughput(throughput);
		this.creationDate = new Date();
		this.lastDecreaseDateTime = new Date();
		this.lastIncreaseDateTime = new Date();

		// Get hash key name
		hashKeyName = keySchema.getHashKeyElement().getAttributeName();
		if (keySchema.getRangeKeyElement() != null) {
			rangeKeyName = keySchema.getRangeKeyElement().getAttributeName();
		}
	}

	public void putItem(Map<String, AttributeValue> item) {
		String hashKeyValue = getHashKeyValue(item);
		if(hashKeyValue != null) {
			items.put(hashKeyValue, item);
		}
	}

	public void removeItem(String hashKey) {
		items.remove(hashKey);
	}

	public Map<String, AttributeValue> getItem(String hashKey) {
		return items.get(hashKey);
	}

	public Map<String, Map<String, AttributeValue>> getItems() {
		return items;
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
		return new Long(items.size());
	}

	public Long getSizeBytes() {
		return new Long(items.size());
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

	protected String getHashKeyValue(Map<String, AttributeValue> item) {
		AttributeValue value = item.get(getHashKeyName());
		if (value.getN() != null) {
			return value.getN();
		} else if (value.getS() != null) {
			return value.getS();
		}
		return null;
	}
}
