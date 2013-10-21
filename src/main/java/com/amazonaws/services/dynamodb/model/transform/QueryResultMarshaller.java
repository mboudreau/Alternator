package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import org.springframework.util.StringUtils;

import java.io.StringWriter;
import java.util.Map;

public class QueryResultMarshaller implements Marshaller<String, QueryResult> {

	public String marshall(QueryResult queryResult) {
		if (queryResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();

			if (queryResult.getCount() != null) {
				jsonWriter.key("Count").value(queryResult.getCount());
			}
			if (queryResult.getItems() != null) {
				jsonWriter.key("Items").array();
				for (Map<String, AttributeValue> value : queryResult.getItems()) {
					jsonWriter.object();
					for (Map.Entry<String, AttributeValue> item : value.entrySet()) {
						String key = item.getKey();
						AttributeValue val = item.getValue();
						jsonWriter.key(key).object();
						if (val.getN() != null) {
							jsonWriter.key("N").value(val.getN());
						} else if (val.getS() != null) {
							jsonWriter.key("S").value(val.getS());
						} else if (val.getNS() != null) {
							jsonWriter.key("NS").value(StringUtils.collectionToCommaDelimitedString(val.getNS()));
						} else if (val.getSS() != null) {
							jsonWriter.key("SS").value(StringUtils.collectionToCommaDelimitedString(val.getSS()));
						}
						jsonWriter.endObject();
					}
					jsonWriter.endObject();
				}
				jsonWriter.endArray();
			}
			if (queryResult.getLastEvaluatedKey() != null) {
				Key key = queryResult.getLastEvaluatedKey();
				jsonWriter.key("LastEvaluatedKey").object();
				AttributeValue value;
				if (key.getHashKeyElement() != null) {
					jsonWriter.key("HashKeyElement").object();
					value = key.getHashKeyElement();
					if (value.getN() != null) {
						jsonWriter.key(value.getN()).value("N");
					} else if (value.getS() != null) {
						jsonWriter.key(value.getS()).value("S");
					}
					jsonWriter.endObject();
				}
				if (key.getRangeKeyElement() != null) {
					jsonWriter.key("RangeKeyElement").object();
					value = key.getRangeKeyElement();
					if (value.getN() != null) {
						jsonWriter.key(value.getN()).value("N");
					} else if (value.getS() != null) {
						jsonWriter.key(value.getS()).value("S");
					}
					jsonWriter.endObject();
				}
				jsonWriter.endObject();
			}
			if (queryResult.getConsumedCapacityUnits() != null) {
				jsonWriter.key("ConsumedCapacityUnits").value(queryResult.getConsumedCapacityUnits());
			}
			jsonWriter.endObject();
			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
