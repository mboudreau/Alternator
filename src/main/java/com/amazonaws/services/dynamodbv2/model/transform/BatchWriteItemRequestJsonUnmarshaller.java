package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.ListUnmarshaller;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import com.fasterxml.jackson.core.JsonToken;

import java.util.List;

public class BatchWriteItemRequestJsonUnmarshaller implements Unmarshaller<BatchWriteItemRequest, JsonUnmarshallerContext> {

    public BatchWriteItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        BatchWriteItemRequest request = new BatchWriteItemRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.getCurrentToken();
        if (token == null) token = context.nextToken();

        while (true) {
            if (token == null) break;

            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("RequestItems", targetDepth)) {
                    request.setRequestItems(new MapUnmarshaller<String, List<WriteRequest>>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), new ListUnmarshaller<WriteRequest>(WriteRequestJsonUnmarshaller.getInstance())).unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static BatchWriteItemRequestJsonUnmarshaller instance;
    public static BatchWriteItemRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new BatchWriteItemRequestJsonUnmarshaller();
        return instance;
    }
}
