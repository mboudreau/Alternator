package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;
import org.springframework.util.StringUtils;

import java.io.StringWriter;
import java.util.Map;

public class PutItemResultMarshaller implements Marshaller<String, PutItemResult> {

	public String marshall(PutItemResult putItemResult) {
		if (putItemResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();

			Map<String, AttributeValue> attr = putItemResult.getAttributes();
			if (attr != null) {
				jsonWriter.key("Attributes").object();

				for (Map.Entry<String, AttributeValue> item : attr.entrySet()) {
					String key = item.getKey();
					AttributeValue value = item.getValue();
					jsonWriter.key(key).object();
					if(value.getN() != null) {
						jsonWriter.key("N").value(value.getN());
					}else if(value.getS() != null) {
						jsonWriter.key("S").value(value.getS());
					}else if(value.getNS() != null) {
						jsonWriter.key("NS").value(StringUtils.collectionToCommaDelimitedString(value.getNS()));
					}else if(value.getSS() != null) {
						jsonWriter.key("SS").value(StringUtils.collectionToCommaDelimitedString(value.getSS()));
					}
					jsonWriter.endObject();
				}

				jsonWriter.endObject();
			}

			if (putItemResult.getConsumedCapacityUnits() != null) {
				jsonWriter.key("ConsumedCapacityUnits").value(putItemResult.getConsumedCapacityUnits());
			}

			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
