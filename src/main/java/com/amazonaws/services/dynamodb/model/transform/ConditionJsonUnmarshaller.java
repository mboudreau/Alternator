package com.amazonaws.services.dynamodb.model.transform;

import com.amazonaws.transform.*;
import org.codehaus.jackson.JsonToken;
import com.amazonaws.services.dynamodb.model.*;


public class ConditionJsonUnmarshaller implements Unmarshaller<Condition, JsonUnmarshallerContext> {

    public Condition unmarshall(JsonUnmarshallerContext context) throws Exception {
        Condition request = new Condition();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

       while (true) {
            if (token == null) break;
            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("AttributeValueList", targetDepth)) {
                    request.setAttributeValueList(new ListUnmarshaller<AttributeValue>(AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
                }
                if (context.testExpression("ComparisonOperator", targetDepth)) {
                    context.nextToken();
                    request.setComparisonOperator(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static ConditionJsonUnmarshaller instance;
    public static ConditionJsonUnmarshaller getInstance() {
        if (instance == null) instance = new ConditionJsonUnmarshaller();
        return instance;
    }
}
