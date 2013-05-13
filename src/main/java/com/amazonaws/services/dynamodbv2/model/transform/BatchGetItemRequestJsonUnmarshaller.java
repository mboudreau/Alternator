package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;
import com.amazonaws.services.dynamodbv2.model.*;

public class BatchGetItemRequestJsonUnmarshaller implements Unmarshaller<BatchGetItemRequest, JsonUnmarshallerContext> {

    public BatchGetItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        BatchGetItemRequest request = new BatchGetItemRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

       while (true) {
            if (token == null) break;
            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("RequestItems", targetDepth)) {
                    request.setRequestItems(new MapUnmarshaller<String, KeysAndAttributes>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), KeysAndAttributesJsonUnmarshaller.getInstance()).unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static BatchGetItemRequestJsonUnmarshaller instance;
    public static BatchGetItemRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new BatchGetItemRequestJsonUnmarshaller();
        return instance;
    }
}
