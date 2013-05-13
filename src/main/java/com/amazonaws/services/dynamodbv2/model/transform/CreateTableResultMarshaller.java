package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;
import java.util.List;

public class CreateTableResultMarshaller implements Marshaller<String, CreateTableResult> {

    public String marshall(CreateTableResult createTableResult) {
        if (createTableResult == null) {
            throw new AmazonClientException("Invalid argument passed to marshall(...)");
        }

        try {
            StringWriter stringWriter = new StringWriter();
            JSONWriter jsonWriter = new JSONWriter(stringWriter);
            jsonWriter.object();

            TableDescription table = createTableResult.getTableDescription();
            if (table != null) {
                jsonWriter.key("TableDescription").object();
                if (table.getCreationDateTime() != null) {
                    jsonWriter.key("CreationDateTime").value(table.getCreationDateTime());
                }
                if (table.getTableName() != null) {
                    jsonWriter.key("TableName").value(table.getTableName());
                }
                if (table.getTableStatus() != null) {
                    jsonWriter.key("TableStatus").value(table.getTableStatus());
                }
                List<AttributeDefinition> attributes = table.getAttributeDefinitions();
                if (attributes != null) {
                    jsonWriter.key("AttributeDefinitions").array();
                    for (AttributeDefinition attr : attributes) {
                        jsonWriter.object();
                        if (attr.getAttributeName() != null) {
                            jsonWriter.key("AttributeName").value(attr.getAttributeName());
                        }
                        if (attr.getAttributeType() != null) {
                            jsonWriter.key("AttributeType").value(attr.getAttributeType());
                        }
                        jsonWriter.endObject(); // AttributeDefinition
                    }
                    jsonWriter.endArray(); // AttributeDefinitions
                }
                List<KeySchemaElement> keySchema = table.getKeySchema();
                if (keySchema != null) {
                    jsonWriter.key("KeySchema").array();
                    for (KeySchemaElement element : keySchema) {
                        jsonWriter.object();
                        if (element.getAttributeName() != null) {
                            jsonWriter.key("AttributeName").value(element.getAttributeName());
                        }
                        if (element.getKeyType() != null) {
                            jsonWriter.key("KeyType").value(element.getKeyType());
                        }
                        jsonWriter.endObject(); // KeySchemaElement
                    }
                    jsonWriter.endArray(); //KeySchema
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
                    jsonWriter.endObject(); //ProvisionedThroughput
                }
                jsonWriter.endObject(); // TableDescription
            }
            jsonWriter.endObject();

            return stringWriter.toString();
        } catch (Throwable t) {
            throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
        }
    }
}
