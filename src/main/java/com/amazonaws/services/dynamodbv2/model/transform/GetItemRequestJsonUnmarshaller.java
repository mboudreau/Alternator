package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.transform.*;
import org.codehaus.jackson.JsonToken;

public class GetItemRequestJsonUnmarshaller implements Unmarshaller<GetItemRequest, JsonUnmarshallerContext> {

    public GetItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        GetItemRequest request = new GetItemRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;
        boolean inArray = false;
        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

        while (true) {
            if (token == null) break;
            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("Key", targetDepth)) {
                    request.setKey(new MapUnmarshaller<String, AttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
                }
                if (context.testExpression("TableName", targetDepth)) {
                    context.nextToken();
                    request.setTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("ConsistentRead", targetDepth)) {
                    request.setConsistentRead(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("AttributesToGet",targetDepth)) {
                    request.setAttributesToGet(new ListUnmarshaller<String>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance()).unmarshall(context));
                }
            }
            else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static GetItemRequestJsonUnmarshaller instance;

    public static GetItemRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new GetItemRequestJsonUnmarshaller();
        return instance;
    }
}
