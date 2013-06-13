package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;
import java.io.StringWriter;
import java.util.Map;
import org.springframework.util.StringUtils;

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
				jsonWriter.key("LastEvaluatedKey").object();
                for (String keyName : queryResult.getLastEvaluatedKey().keySet()) {
                    jsonWriter.key(keyName).object();
                    AttributeValue keyAttrValue = queryResult.getLastEvaluatedKey().get(keyName);
                    if (keyAttrValue.getN() != null) {
                        jsonWriter.key("N").value(keyAttrValue.getN());
                    } else if (keyAttrValue.getS() != null) {
                        jsonWriter.key("S").value(keyAttrValue.getS());
                    } else if (keyAttrValue.getNS() != null) {
                        jsonWriter.key("NS").value(StringUtils.collectionToCommaDelimitedString(keyAttrValue.getNS()));
                    } else if (keyAttrValue.getSS() != null) {
                        jsonWriter.key("SS").value(StringUtils.collectionToCommaDelimitedString(keyAttrValue.getSS()));
                    }
                    jsonWriter.endObject();
                }
                jsonWriter.endObject();
			}

			if (queryResult.getConsumedCapacity() != null) {
				jsonWriter.key("ConsumedCapacity").object();
				jsonWriter.key("TableName").value(queryResult.getConsumedCapacity().getTableName());
				jsonWriter.key("CapacityUnits").value(queryResult.getConsumedCapacity().getCapacityUnits());
				jsonWriter.endObject();
			}

            jsonWriter.endObject();
			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
