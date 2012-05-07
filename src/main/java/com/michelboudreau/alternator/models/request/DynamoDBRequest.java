package com.michelboudreau.alternator.models.request;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamoDBRequest {
	private String tableName;
	private Set<HashKeyElement> keySchema;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Set<HashKeyElement> getKeySchema() {
		return keySchema;
	}

	public void setKeySchema(Set<HashKeyElement> keySchema) {
		this.keySchema = keySchema;
	}

	/*public HashKeyElement getHashKey(String name) {
		return this.keySchema;
	}*/
}
