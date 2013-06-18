package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

import static org.codehaus.jackson.JsonToken.*;

public class KeySchemaElementJsonUnmarshaller implements Unmarshaller<KeySchemaElement, JsonUnmarshallerContext>
{
    public KeySchemaElement unmarshall(JsonUnmarshallerContext context) throws Exception {
        KeySchemaElement element = new KeySchemaElement();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

       while (true) {
            if (token == null) break;
            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("AttributeName", targetDepth)) {
					context.nextToken();
                    element.setAttributeName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
	            if (context.testExpression("KeyType", targetDepth)) {
					context.nextToken();
                    element.setKeyType(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return element;
    }

    private static KeySchemaElementJsonUnmarshaller instance;
    public static KeySchemaElementJsonUnmarshaller getInstance() {
        if (instance == null) instance = new KeySchemaElementJsonUnmarshaller();
        return instance;
    }
}
