package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;

public class ListTablesResultMarshaller implements Marshaller<String, ListTablesResult> {

	public String marshall(ListTablesResult listTablesResult) {
		if (listTablesResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		/*
		{"TableNames":["Table1","Table2","Table3"], "LastEvaluatedTableName":"Table3"}
		 */
		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();

			if(listTablesResult != null) {
				jsonWriter.key("TableNames").array();
				for(String tableName:listTablesResult.getTableNames()) {
					jsonWriter.value(tableName);
				}
				jsonWriter.endArray(); // TableNames

				if(listTablesResult.getLastEvaluatedTableName() != null) {
					jsonWriter.key("LastEvaluatedTableName").value(listTablesResult.getLastEvaluatedTableName());
				}
			}

			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
