package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.AttributeValue;

import java.util.Map;

public class Item {
	private Map<String, AttributeValue> attributes;

	public Item(Map<String, AttributeValue> attributes) {
		this.attributes = attributes;
	}

	public Map<String, AttributeValue> getAttributes() {
		return attributes;
	}

	public void updateAttributes(Map<String, AttributeValue> attributes) {
		
	}
}
