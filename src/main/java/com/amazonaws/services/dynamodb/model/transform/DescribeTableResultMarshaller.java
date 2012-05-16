package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;

public class DescribeTableResultMarshaller implements Marshaller<String, DescribeTableResult> {

	public String marshall(DescribeTableResult describeTableResult) {
		if (describeTableResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();

			TableDescription table = describeTableResult.getTable();
			if (table != null) {
				jsonWriter.key("Table").object();
				jsonWriter.key("ItemCount").value(table.getItemCount());
				jsonWriter.key("TableName").value(table.getTableName());
				jsonWriter.key("TableSizeBytes").value(table.getTableSizeBytes());
				jsonWriter.key("TableStatus").value(table.getTableStatus());
				jsonWriter.key("CreationDateTime").value(table.getCreationDateTime());

				KeySchema keySchema = table.getKeySchema();
				if (keySchema != null) {
					jsonWriter.key("KeySchema").object();
					KeySchemaElement hashKeyElement = keySchema.getHashKeyElement();
					if (hashKeyElement != null) {
						jsonWriter.key("HashKeyElement").object();
						if (hashKeyElement.getAttributeName() != null) {
							jsonWriter.key("AttributeName").value(hashKeyElement.getAttributeName());
						}
						if (hashKeyElement.getAttributeType() != null) {
							jsonWriter.key("AttributeType").value(hashKeyElement.getAttributeType());
						}
						jsonWriter.endObject();
					}

					KeySchemaElement rangeKeyElement = keySchema.getRangeKeyElement();
					if (rangeKeyElement != null) {
						jsonWriter.key("RangeKeyElement").object();
						if (rangeKeyElement.getAttributeName() != null) {
							jsonWriter.key("AttributeName").value(rangeKeyElement.getAttributeName());
						}
						if (rangeKeyElement.getAttributeType() != null) {
							jsonWriter.key("AttributeType").value(rangeKeyElement.getAttributeType());
						}
						jsonWriter.endObject();
					}
					jsonWriter.endObject();
				}

				ProvisionedThroughputDescription provisionedThroughput = table.getProvisionedThroughput();
				if (provisionedThroughput != null) {
					jsonWriter.key("ProvisionedThroughput").object();
					if (provisionedThroughput.getReadCapacityUnits() != null) {
						jsonWriter.key("ReadCapacityUnits").value(provisionedThroughput.getReadCapacityUnits());
					}
					if (provisionedThroughput.getWriteCapacityUnits() != null) {
						jsonWriter.key("WriteCapacityUnits").value(provisionedThroughput.getWriteCapacityUnits());
					}
					if (provisionedThroughput.getLastDecreaseDateTime() != null) {
						jsonWriter.key("LastDecreaseDateTime").value(provisionedThroughput.getLastDecreaseDateTime());
					}
					if (provisionedThroughput.getLastIncreaseDateTime() != null) {
						jsonWriter.key("LastIncreaseDateTime").value(provisionedThroughput.getLastIncreaseDateTime());
					}
					jsonWriter.endObject();
				}

				jsonWriter.endObject();
			}

			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
