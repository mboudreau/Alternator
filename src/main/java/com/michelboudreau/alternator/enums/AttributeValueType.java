package com.michelboudreau.alternator.enums;

public enum AttributeValueType {
	// Item Actions
	S("S"),
	N("N"),
	NS("NS"),
	SS("SS"),

	// Unknowns
	UNKNOWN("");

	private String type;

	AttributeValueType(String value) {
		this.type = value;
	}

	public String toString() {
		return type;
	}

	public static AttributeValueType fromString(String value) {
        for (AttributeValueType t : values() ){
            if (t.toString().equals(value)) return t;
        }
        return UNKNOWN;
    }
}
