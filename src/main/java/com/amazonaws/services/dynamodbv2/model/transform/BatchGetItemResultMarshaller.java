package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

public class BatchGetItemResultMarshaller implements Marshaller<String, BatchGetItemResult> {

	public String marshall(BatchGetItemResult batchGetItemResult) {
		if (batchGetItemResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);

            //Begin whole object containing both response and unprocessed keys!
			jsonWriter.object();

            //Begin response object
			jsonWriter.key("Responses").object();

            Map<String, List<Map<String, AttributeValue>>> responses = batchGetItemResult.getResponses();

			if (responses != null) {
				for (String tableKey : responses.keySet()) {

                    //Begin each table object
					jsonWriter.key(tableKey).array();
                    List<Map<String, AttributeValue>> items = responses.get(tableKey);
					if (items != null) {

                        //Begin array items (a series of items)
						for (Map<String, AttributeValue> item : items) {

                            //Begin each objects constituting array items.No keys in these objects
							jsonWriter.object();
							for (String itemKey : item.keySet()) {

                                //Begin attribute of each item
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
                                //End attribute of each item
								jsonWriter.endObject();
							}
                            //End each objects constituting array items.No keys in these objects
							jsonWriter.endObject();
						}
                        //End array items (a series of items)
					}
                    //End each table array
                    jsonWriter.endArray();
				}

			}
            //End response object
			jsonWriter.endObject();
            //End whole object containing both response and unprocessed keys!
			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
