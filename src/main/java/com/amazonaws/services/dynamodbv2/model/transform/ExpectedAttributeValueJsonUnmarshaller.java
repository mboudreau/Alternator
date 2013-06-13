package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

public class ExpectedAttributeValueJsonUnmarshaller implements Unmarshaller<ExpectedAttributeValue, JsonUnmarshallerContext> {

    public ExpectedAttributeValue unmarshall(JsonUnmarshallerContext context) throws Exception {
        ExpectedAttributeValue request = new ExpectedAttributeValue();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

       while (true) {
            if (token == null) break;
            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("Value", targetDepth)) {
                    request.setValue(AttributeValueJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("Exists", targetDepth)) {
                    context.nextToken();
                    request.setExists(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static ExpectedAttributeValueJsonUnmarshaller instance;
    public static ExpectedAttributeValueJsonUnmarshaller getInstance() {
        if (instance == null) instance = new ExpectedAttributeValueJsonUnmarshaller();
        return instance;
    }
}
