package com.michelboudreau.alternator.enums;

public enum RequestType {
	PUT("PutItem"),
	GET("GetItem"),
	QUERY("Query"),
	SCAN("Scan"),
	CREATE_TABLE("CreateTable"),
	DELETE_TABLE("DeleteTable"),
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
