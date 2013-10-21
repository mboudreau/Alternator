package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteResponse;
import com.amazonaws.services.dynamodb.model.DeleteRequest;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.PutRequest;
import com.amazonaws.services.dynamodb.model.WriteRequest;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

public class BatchWriteItemResultMarshaller implements Marshaller<String, BatchWriteItemResult> {

	public String marshall(BatchWriteItemResult batchWriteItemResult) {
		if (batchWriteItemResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

		try {
			StringWriter stringWriter = new StringWriter();
			JSONWriter jsonWriter = new JSONWriter(stringWriter);

            //Begin whole object containing responses and unprocessedItems.
			jsonWriter.object();

            //Begin response object
			jsonWriter.key("Responses").object();

			Map<String, BatchWriteResponse> responses = batchWriteItemResult.getResponses();

			if (responses != null) {
				for (String tableKey : responses.keySet()) {
                    //Begin table
					jsonWriter.key(tableKey).object();
                    Double consumedCapacityUnits = responses.get(tableKey).getConsumedCapacityUnits();
					if (consumedCapacityUnits != null) {
						jsonWriter.key("ConsumedCapacityUnits").value(consumedCapacityUnits);
					}
                    //End table
					jsonWriter.endObject();
				}
			}
            //End response
            jsonWriter.endObject();

            //Begin unprocessedItems
			jsonWriter.key("UnprocessedItems").object();
            Map<String, List<WriteRequest>> unprocessedItems = batchWriteItemResult.getUnprocessedItems();
            if (unprocessedItems != null) {
                for (String tableKey : unprocessedItems.keySet()) {
                    //begin table
                    jsonWriter.key(tableKey).array();
                    for (WriteRequest request : unprocessedItems.get(tableKey)) {
                        //begin each object of array
                        jsonWriter.object();
                        PutRequest putRequest = request.getPutRequest();
                        DeleteRequest deleteRequest = request.getDeleteRequest();
                        if (putRequest != null) {
                            //Begin put request
                            jsonWriter.key("PutRequest").object();
                            Map<String, AttributeValue> item = putRequest.getItem();
                            if (item != null) {
                                for (String itemKey : item.keySet()) {
                                    //begin attribute value
                                    jsonWriter.key(itemKey).object();
                                    AttributeValue value = item.get(itemKey);
                                    if (value.getN() != null) {
                                        jsonWriter.key("N").value(value.getN());
                                    } else if (value.getS() != null) {
                                        jsonWriter.key("S").value(value.getS());
                                    } else if (value.getB() != null) {
                                        jsonWriter.key("B").value(value.getS());
                                    } else if (value.getSS() != null) {
                                        jsonWriter.key("SS").value(value.getS());
                                    } else if (value.getNS() != null) {
                                        jsonWriter.key("NS").value(value.getNS());
                                    } else if (value.getBS() != null) {
                                        jsonWriter.key("BS").value(value.getSS());
                                    }
                                    //end attribute value
                                    jsonWriter.endObject();
                                }
                            }
                            //end put request
                            jsonWriter.endObject();
                        } else if (deleteRequest != null) {
                            //Begin delete request
                            jsonWriter.key("DeleteRequest").object();
                            Key key = deleteRequest.getKey();
                            if (key != null) {
                                //begin key
                                jsonWriter.key("Key").object();
                                AttributeValue hashKeyElemenet = key.getHashKeyElement();
                                AttributeValue rangeKeyElement = key.getRangeKeyElement();
                                if (hashKeyElemenet != null) {
                                    if (hashKeyElemenet.getN() != null) {
                                        jsonWriter.key("N").value(hashKeyElemenet.getN());
                                    } else if (hashKeyElemenet.getS() != null) {
                                        jsonWriter.key("S").value(hashKeyElemenet.getS());
                                    } else if (hashKeyElemenet.getB() != null) {
                                        jsonWriter.key("B").value(hashKeyElemenet.getS());
                                    } else if (hashKeyElemenet.getSS() != null) {
                                        jsonWriter.key("SS").value(hashKeyElemenet.getS());
                                    } else if (hashKeyElemenet.getNS() != null) {
                                        jsonWriter.key("NS").value(hashKeyElemenet.getNS());
                                    } else if (hashKeyElemenet.getBS() != null) {
                                        jsonWriter.key("BS").value(hashKeyElemenet.getSS());
                                    }
                                }
                                if (rangeKeyElement != null) {
                                    if (rangeKeyElement.getN() != null) {
                                        jsonWriter.key("N").value(rangeKeyElement.getN());
                                    } else if (rangeKeyElement.getS() != null) {
                                        jsonWriter.key("S").value(rangeKeyElement.getS());
                                    } else if (rangeKeyElement.getB() != null) {
                                        jsonWriter.key("B").value(rangeKeyElement.getS());
                                    } else if (rangeKeyElement.getSS() != null) {
                                        jsonWriter.key("SS").value(rangeKeyElement.getS());
                                    } else if (rangeKeyElement.getNS() != null) {
                                        jsonWriter.key("NS").value(rangeKeyElement.getNS());
                                    } else if (rangeKeyElement.getBS() != null) {
                                        jsonWriter.key("BS").value(rangeKeyElement.getSS());
                                    }
                                }
                                //end key
                                jsonWriter.endObject();
                            }
                            //End delete request
                            jsonWriter.endObject();
                        }
                        //end each object of array
                        jsonWriter.endObject();
                    }
                    //end table
                    jsonWriter.endArray();
                }
            }

            //End unprocessedItems
			jsonWriter.endObject();

            //End whole object containing responses and unprocessedItems.
			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
