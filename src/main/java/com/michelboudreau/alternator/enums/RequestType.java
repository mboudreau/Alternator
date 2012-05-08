package com.michelboudreau.alternator.enums;

public enum RequestType {
	// Item Actions
	PUT("PutItem"),
	GET("GetItem"),
	UPDATE("UpdateItem"),
	DELETE("DeleteItem"),
	BATCH_GET_ITEM("BatchGetItem"),
	BATCH_WRITE_ITEM("BatchWriteItem"),

	// Operations
	QUERY("Query"),
	SCAN("Scan"),

	// Table Actions
	CREATE_TABLE("CreateTable"),
	DESCRIBE_TABLE("DescribeTable"),
	LIST_TABLES("ListTables"),
	UPDATE_TABLE("UpdateTable"),
	DELETE_TABLE("DeleteTable"),

	// Unknowns
	UNKNOWN("");

	private String type;

	RequestType(String value) {
		this.type = value;
	}

	public String toString() {
		return type;
	}

	public static RequestType fromString(String value) {
        for (RequestType t : values() ){
            if (t.toString().equals(value)) return t;
        }
        return UNKNOWN;
    }
}
