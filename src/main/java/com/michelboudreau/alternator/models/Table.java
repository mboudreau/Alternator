package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.*;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Table {

	private String name;
	private KeySchema keySchema;
	private ProvisionedThroughputDescription throughputDescription;
	private Date lastDecreaseDateTime;
	private Date lastIncreaseDateTime;
//    private Map<String, Map<String, AttributeValue>> items = new HashMap<String, Map<String, AttributeValue>>();
    private Map<String, ItemRangeGroup> itemRangeGroups = new HashMap<String, ItemRangeGroup>();
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
            ItemRangeGroup rangeGroup = itemRangeGroups.get(hashKeyValue);
            if (rangeGroup == null)
            {
                rangeGroup = new ItemRangeGroup();
                itemRangeGroups.put(hashKeyValue, rangeGroup);
            }
            String rangeKeyValue = getRangeKeyValue(item);
            rangeGroup.putItem(rangeKeyValue, item);
        }
	}

	public void removeItem(String hashKey) {
		itemRangeGroups.remove(hashKey);
	}

	public void removeItem(String hashKey, String rangeKey) {
        ItemRangeGroup rangeGroup = itemRangeGroups.get(hashKey);
        if (rangeGroup != null)
        {
            rangeGroup.removeItem(rangeKey);
        }
	}

	public ItemRangeGroup getItemRangeGroup(String hashKey) {
		return itemRangeGroups.get(hashKey);
	}

//	public Map<String, AttributeValue> getItem(String hashKey) {
//		return getItem(hashKey, null);
//	}

	public Map<String, AttributeValue> getItem(String hashKey, String rangeKey) {
        ItemRangeGroup rangeGroup = itemRangeGroups.get(hashKey);
        if (rangeGroup != null)
        {
            return rangeGroup.getItem(rangeKey);
        }
        return null;
	}

	public Map<String, ItemRangeGroup> getItemRangeGroups() {
        return itemRangeGroups;
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
        long count = 0;
        Collection<ItemRangeGroup> rangeGroups = itemRangeGroups.values();
        for (ItemRangeGroup rangeGroup : rangeGroups) {
            count += rangeGroup.size();
        }
		return new Long(count);
	}

	public Long getSizeBytes() {
        // Use an artificially assumed size for each item as a mock size.
		return 100 * getItemCount();
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
        AttributeValue value = item.get(hashKeyName);
        if (value != null) {
            if (value.getN() != null) {
                return value.getN();
            } else if (value.getS() != null) {
                return value.getS();
            }
        }
        return null;
	}

	protected String getRangeKeyValue(Map<String, AttributeValue> item) {
        if (rangeKeyName != null) {
            AttributeValue value = item.get(rangeKeyName);
            if (value != null) {
                if (value.getN() != null) {
                    return value.getN();
                } else if (value.getS() != null) {
                    return value.getS();
                }
            }
        }
        return null;
	}
}
