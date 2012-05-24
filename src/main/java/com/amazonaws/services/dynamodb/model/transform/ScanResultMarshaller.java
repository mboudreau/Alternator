package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;
import java.util.Map;

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
                jsonWriter.endObject();
            }
            jsonWriter.endArray();
            jsonWriter.key("LastEvaluatedKey").object();
            jsonWriter.key("HashKeyElement").object();
            if (scanResult.getLastEvaluatedKey().getHashKeyElement().getS() != null) {
                jsonWriter.key("S").value(scanResult.getLastEvaluatedKey().getHashKeyElement().getS());
            } else if (scanResult.getLastEvaluatedKey().getHashKeyElement().getN() != null) {
                jsonWriter.key("N").value(scanResult.getLastEvaluatedKey().getHashKeyElement().getN());
            } else if (scanResult.getLastEvaluatedKey().getHashKeyElement().getSS() != null) {
                jsonWriter.key("SS").value(scanResult.getLastEvaluatedKey().getHashKeyElement().getSS());
            } else if (scanResult.getLastEvaluatedKey().getHashKeyElement().getNS() != null) {
                jsonWriter.key("NS").value(scanResult.getLastEvaluatedKey().getHashKeyElement().getNS());
            }
            jsonWriter.endObject();
            jsonWriter.key("RangeKeyElement").object();
            if (scanResult.getLastEvaluatedKey().getRangeKeyElement() != null) {
                if (scanResult.getLastEvaluatedKey().getRangeKeyElement().getS() != null) {
                    jsonWriter.key("S").value(scanResult.getLastEvaluatedKey().getRangeKeyElement().getS());
                } else if (scanResult.getLastEvaluatedKey().getRangeKeyElement().getN() != null) {
                    jsonWriter.key("N").value(scanResult.getLastEvaluatedKey().getRangeKeyElement().getN());
                } else if (scanResult.getLastEvaluatedKey().getRangeKeyElement().getSS() != null) {
                    jsonWriter.key("SS").value(scanResult.getLastEvaluatedKey().getRangeKeyElement().getSS());
                } else if (scanResult.getLastEvaluatedKey().getRangeKeyElement().getNS() != null) {
                    jsonWriter.key("NS").value(scanResult.getLastEvaluatedKey().getRangeKeyElement().getNS());
                }
            }
            jsonWriter.endObject();
            jsonWriter.endObject();
            jsonWriter.key("ConsumedCapacityUnits").value(scanResult.getConsumedCapacityUnits());
            jsonWriter.key("ScannedCount").value(scanResult.getScannedCount());

            jsonWriter.endObject();

            return stringWriter.toString();
        } catch (Throwable t) {
            throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
        }
    }
}
