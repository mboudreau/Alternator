package com.amazonaws.services.dynamodb.model.transform;

import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

public class GetItemRequestJsonUnmarshaller implements Unmarshaller<GetItemRequest, JsonUnmarshallerContext> {

    public GetItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        GetItemRequest request = new GetItemRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();
/*
        while (true) {
            if (token == null) break;

            if (token == FIELD_NAME || token == START_OBJECT) {
                if (context.testExpression("Items", targetDepth)) {
                    request.setItem(new MapUnmarshaller<String,AttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
                }
	            if (context.testExpression("TableName", targetDepth)) {
                    request.setTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
	             if (context.testExpression("ExpectedAttributeValue", targetDepth)) {
                   *//* putItemRequest.setExpected(new MapUnmarshaller<String,ExpectedAttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), Expect.getInstance()).unmarshall(context));*//*
                }
            } else if (token == END_ARRAY || token == END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }*/

        return request;
    }

    private static GetItemRequestJsonUnmarshaller instance;
    public static GetItemRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new GetItemRequestJsonUnmarshaller();
        return instance;
    }
}
