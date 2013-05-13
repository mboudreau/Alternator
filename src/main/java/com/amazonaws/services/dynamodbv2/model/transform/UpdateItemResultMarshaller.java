package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;
import java.io.StringWriter;

public class UpdateItemResultMarshaller implements Marshaller<String, UpdateItemResult> {

	public String marshall(UpdateItemResult updateItemResult) {
		if (updateItemResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}
		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();
			jsonWriter.key("Attributes").object();
			for (String key : updateItemResult.getAttributes().keySet()) {
				jsonWriter.key(key).object();
				if (updateItemResult.getAttributes().get(key) != null) {
					if (updateItemResult.getAttributes().get(key).getS() != null) {
						jsonWriter.key("S").value(updateItemResult.getAttributes().get(key).getS());
					} else if (updateItemResult.getAttributes().get(key).getN() != null) {
						jsonWriter.key("N").value(updateItemResult.getAttributes().get(key).getN());
					} else if (updateItemResult.getAttributes().get(key).getSS() != null) {
						jsonWriter.key("SS").value(updateItemResult.getAttributes().get(key).getSS());
					} else if (updateItemResult.getAttributes().get(key).getNS() != null) {
						jsonWriter.key("NS").value(updateItemResult.getAttributes().get(key).getNS());
					}
				}
				jsonWriter.endObject();
			}
			jsonWriter.endObject();

			if (updateItemResult.getConsumedCapacity() != null) {
				jsonWriter.key("ConsumedCapacity").object();
				jsonWriter.key("TableName").value(updateItemResult.getConsumedCapacity().getTableName());
				jsonWriter.key("CapacityUnits").value(updateItemResult.getConsumedCapacity().getCapacityUnits());
				jsonWriter.endObject();
			}

            // TODO: ItemCollectionMetrics — (map)
            //       Information about item collections, if any, that were affected by the operation. ItemCollectionMetrics is only returned if it was asked for in the request. If the table does not have any secondary indexes, this information is not returned in the response.

            jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
