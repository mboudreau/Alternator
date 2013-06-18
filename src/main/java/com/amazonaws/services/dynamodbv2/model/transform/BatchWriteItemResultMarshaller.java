package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.*;
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
                                    jsonWriter.endObject(); // itemKey
                                }
                            }
                            //end put request
                            jsonWriter.endObject(); // PutRequest
                        } else if (deleteRequest != null) {
                            //Begin delete request
                            jsonWriter.key("DeleteRequest").object();
                            Map<String, AttributeValue> keyMap = deleteRequest.getKey();
                            if (keyMap != null) {
                                //begin key
                                jsonWriter.key("Key").object();

                                for (String keyName : keyMap.keySet()) {
                                    AttributeValue keyAttrValue = keyMap.get(keyName);
                                    jsonWriter.key(keyName).object();
                                    if (keyAttrValue.getN() != null) {
                                        jsonWriter.key("N").value(keyAttrValue.getN());
                                    } else if (keyAttrValue.getS() != null) {
                                        jsonWriter.key("S").value(keyAttrValue.getS());
                                    } else if (keyAttrValue.getB() != null) {
                                        jsonWriter.key("B").value(keyAttrValue.getS());
                                    } else if (keyAttrValue.getSS() != null) {
                                        jsonWriter.key("SS").value(keyAttrValue.getS());
                                    } else if (keyAttrValue.getNS() != null) {
                                        jsonWriter.key("NS").value(keyAttrValue.getNS());
                                    } else if (keyAttrValue.getBS() != null) {
                                        jsonWriter.key("BS").value(keyAttrValue.getSS());
                                    }
                                    //end keyName
                                    jsonWriter.endObject(); // keyName
                                }
                                //end key
                                jsonWriter.endObject(); // Key
                            }
                            //End delete request
                            jsonWriter.endObject(); // DeleteRequest
                        }
                        //end each object of array
                        jsonWriter.endObject(); // UnprocessedItem
                    }
                    //end table
                    jsonWriter.endArray(); // tableKey
                }
            }

            //End unprocessedItems
			jsonWriter.endObject(); // UnprocessedItems

            // TODO: ItemCollectionMetrics — (map<Array<map>>)
            //       A list of tables that were processed by BatchWriteItem and, for each table, information about any item collections that were affected by individual DeleteItem or PutItem operations.

            //Begin ConsumedCapacity object
			jsonWriter.key("ConsumedCapacity").object();

			List<ConsumedCapacity> consumedCapacities = batchWriteItemResult.getConsumedCapacity();

			if (consumedCapacities != null) {
				for (ConsumedCapacity consumedCapacity : consumedCapacities) {
					jsonWriter.key(consumedCapacity.getTableName()).object();
                    Double consumedCapacityUnits = consumedCapacity.getCapacityUnits();
					if (consumedCapacityUnits != null) {
						jsonWriter.key("CapacityUnits").value(consumedCapacityUnits);
					}
                    jsonWriter.endObject(); // tableName
				}
			}
            //End ConsumedCapacity
            jsonWriter.endObject(); // ConsumedCapacity

            //End whole object containing responses and unprocessedItems.
			jsonWriter.endObject();

			return stringWriter.toString();
		} catch (Throwable t) {
			throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
		}
	}
}
