package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;

public class CreateTableResultMarshaller implements Marshaller<String, CreateTableResult> {

	public String marshall(CreateTableResult createTableResult) {
		if (createTableResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();

			if (createTableResult.getTableDescription() != null) {
				jsonWriter.key("TableDescription").object();
				TableDescription desc = createTableResult.getTableDescription();
				if (desc.getCreationDateTime() != null) {
					jsonWriter.key("CreationDateTime").value(desc.getCreationDateTime());
				}
				if (desc.getTableName() != null) {
					jsonWriter.key("TableName").value(desc.getTableName());
				}
				if (desc.getTableStatus() != null) {
					jsonWriter.key("TableStatus").value(desc.getTableStatus());
				}

				KeySchema keySchema = desc.getKeySchema();
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
				ProvisionedThroughputDescription provisionedThroughput = desc.getProvisionedThroughput();
				if (provisionedThroughput != null) {
					jsonWriter.key("ProvisionedThroughput").object();
					if (provisionedThroughput.getReadCapacityUnits() != null) {
						jsonWriter.key("ReadCapacityUnits").value(provisionedThroughput.getReadCapacityUnits());
					}
					if (provisionedThroughput.getWriteCapacityUnits() != null) {
						jsonWriter.key("WriteCapacityUnits").value(provisionedThroughput.getWriteCapacityUnits());
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
