package com.amazonaws.services.dynamodb.model.transform;

import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;


public class KeyJsonUnmarshaller implements Unmarshaller<Key, JsonUnmarshallerContext> {

    public Key unmarshall(JsonUnmarshallerContext context) throws Exception {
        Key request = new Key();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

       while (true) {
            if (token == null) break;
            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("HashKeyElement", targetDepth)) {
                    request.setHashKeyElement(AttributeValueJsonUnmarshaller.getInstance().unmarshall(context));
                }
	            if (context.testExpression("RangeKeyElement", targetDepth)) {
                    request.setRangeKeyElement(AttributeValueJsonUnmarshaller.getInstance().unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static KeyJsonUnmarshaller instance;
    public static KeyJsonUnmarshaller getInstance() {
        if (instance == null) instance = new KeyJsonUnmarshaller();
        return instance;
    }
}
