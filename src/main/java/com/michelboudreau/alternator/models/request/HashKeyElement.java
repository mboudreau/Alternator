package com.michelboudreau.alternator.models.request;

import org.codehaus.jackson.annotate.JsonProperty;

public class HashKeyElement {
	private String attributeName;
	private String attributeType;

	public String getAttributeName() {
		return attributeName;
	}

	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	public String getAttributeType() {
		return attributeType;
	}

	public void setAttributeType(String attributeType) {
		this.attributeType = attributeType;
	}
}
