package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import com.fasterxml.jackson.core.JsonToken;

import java.util.Map;

public class UpdateItemRequestJsonUnmarshaller implements Unmarshaller<UpdateItemRequest, JsonUnmarshallerContext> {

    public UpdateItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        UpdateItemRequest request = new UpdateItemRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();
        while (true) {
            if (token == null) break;

            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("TableName", targetDepth)) {
                    context.nextToken();
                    request.setTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("Key", targetDepth)) {
                    request.setKey(new MapUnmarshaller<String, AttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
                }
                if (context.testExpression("Expected", targetDepth)) {
                	Map<String, ExpectedAttributeValue> expected = new MapUnmarshaller<String, ExpectedAttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), ExpectedAttributeValueJsonUnmarshaller.getInstance()).unmarshall(context);
                    request.setExpected(expected);
                }if (context.testExpression("AttributeUpdates", targetDepth)) {
                    request.setAttributeUpdates(new MapUnmarshaller<String, AttributeValueUpdate>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueUpdateJsonUnmarshaller.getInstance()).unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }
        return request;
    }

    private static UpdateItemRequestJsonUnmarshaller instance;
    public static UpdateItemRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new UpdateItemRequestJsonUnmarshaller();
        return instance;
    }
}
