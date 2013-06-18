package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;

public class AmazonServiceExceptionMarshaller implements Marshaller<String, AmazonServiceException> {

	public String marshall(AmazonServiceException exception) {
		if (exception == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);
			jsonWriter.object();

			jsonWriter.key("__type").value("com.amazonaws.dynamodb.v20120810#" + exception.getClass().getSimpleName());
			jsonWriter.key("message").value(exception.getMessage());

			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
