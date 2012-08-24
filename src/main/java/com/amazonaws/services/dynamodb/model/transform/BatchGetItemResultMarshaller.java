package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;
import java.util.Map;

public class BatchGetItemResultMarshaller implements Marshaller<String, BatchGetItemResult> {

	public String marshall(BatchGetItemResult batchGetItemResult) {
		if (batchGetItemResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();
			jsonWriter.key("Responses").object();

			Map<String, BatchResponse> responses = batchGetItemResult.getResponses();
			if (responses != null) {
				for (String tableKey : responses.keySet()) {
					jsonWriter.key(tableKey).object();
					if (responses.get(tableKey).getItems() != null) {
						jsonWriter.key("Items").array();
						for (Map<String, AttributeValue> item : responses.get(tableKey).getItems()) {
							jsonWriter.object();
							for (String itemKey : item.keySet()) {
								jsonWriter.key(itemKey).object();
								AttributeValue value = item.get(itemKey);
								if (value.getN() != null) {
									jsonWriter.key("N").value(value.getN());
								} else if (value.getS() != null) {
									jsonWriter.key("S").value(value.getS());
								} else if (value.getNS() != null) {
									jsonWriter.key("NS").value(value.getNS());
								} else if (value.getSS() != null) {
									jsonWriter.key("SS").value(value.getSS());
								}
								jsonWriter.endObject();
							}

							jsonWriter.endObject();
						}
						jsonWriter.endArray();
						jsonWriter.key("ConsumedCapacityUnits").value(1);
					}
					jsonWriter.endObject();
				}

			}
			jsonWriter.key("UnprocessedKeys").value("");
			jsonWriter.endObject();
			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
