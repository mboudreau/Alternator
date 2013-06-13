package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;
import java.io.StringWriter;
import java.util.Map;
import org.springframework.util.StringUtils;

public class ScanResultMarshaller implements Marshaller<String, ScanResult> {

	public String marshall(ScanResult scanResult) {
		if (scanResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}
		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();
			jsonWriter.key("Count").value(scanResult.getCount());
			jsonWriter.key("Items").array();
			for (Map<String, AttributeValue> item : scanResult.getItems()) {
				jsonWriter.object();
				for (String k : item.keySet()) {
					if (item.get(k) != null) {
						jsonWriter.key(k).object();
						if (item.get(k).getS() != null) {
							jsonWriter.key("S").value(item.get(k).getS());
						} else if (item.get(k).getN() != null) {
							jsonWriter.key("N").value(item.get(k).getN());
						} else if (item.get(k).getSS() != null) {
							jsonWriter.key("SS").value(item.get(k).getSS());
						} else if (item.get(k).getNS() != null) {
							jsonWriter.key("NS").value(item.get(k).getNS());
						}
						jsonWriter.endObject();
					}
				}
				jsonWriter.endObject();
			}
			jsonWriter.endArray();

			if (scanResult.getLastEvaluatedKey() != null) {
				jsonWriter.key("LastEvaluatedKey").object();
                for (String keyName : scanResult.getLastEvaluatedKey().keySet()) {
                    jsonWriter.key(keyName).object();
                    AttributeValue keyAttrValue = scanResult.getLastEvaluatedKey().get(keyName);
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

			if (scanResult.getConsumedCapacity() != null) {
				jsonWriter.key("ConsumedCapacity").object();
				jsonWriter.key("TableName").value(scanResult.getConsumedCapacity().getTableName());
				jsonWriter.key("CapacityUnits").value(scanResult.getConsumedCapacity().getCapacityUnits());
				jsonWriter.endObject();
			}

			jsonWriter.key("ScannedCount").value(scanResult.getScannedCount());

			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
