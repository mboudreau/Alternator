package com.amazonaws.services.dynamodb.model.transform;

import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;
import com.amazonaws.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;

public class DeleteItemRequestJsonUnmarshaller implements Unmarshaller<DeleteItemRequest, JsonUnmarshallerContext> {

    public DeleteItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        DeleteItemRequest request = new DeleteItemRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

        while (true) {
            if (token == null) break;

            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("Expected", targetDepth)) {
                    Map<String, AttributeValue> map = new MapUnmarshaller<String, AttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context);
                    Map<String, ExpectedAttributeValue> expected = new HashMap<String,ExpectedAttributeValue>();
                    for(String key : map.keySet()){
                        ExpectedAttributeValue value = new ExpectedAttributeValue();
                        value.setValue(map.get(key));
                        value.setExists(true);
                        expected.put(key,value);
                    }
                    request.setExpected(expected);
                }

            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static DeleteItemRequestJsonUnmarshaller instance;
    public static DeleteItemRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new DeleteItemRequestJsonUnmarshaller();
        return instance;
    }
}
