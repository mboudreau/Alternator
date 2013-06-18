package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.transform.*;
import org.codehaus.jackson.JsonToken;

public class ScanRequestJsonUnmarshaller implements Unmarshaller<ScanRequest, JsonUnmarshallerContext> {

    public ScanRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        ScanRequest request = new ScanRequest();

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
                if (context.testExpression("Limit", targetDepth)) {
                    context.nextToken();
                    request.setLimit(SimpleTypeJsonUnmarshallers.IntegerJsonUnmarshaller.getInstance().unmarshall(context));
                }if (context.testExpression("ScanFilter", targetDepth)) {
                    request.setScanFilter(new MapUnmarshaller<String, Condition>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), ConditionJsonUnmarshaller.getInstance()).unmarshall(context));
                }
                if (context.testExpression("ExclusiveStartKey", targetDepth)) {
                    request.setExclusiveStartKey(new MapUnmarshaller<String, AttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
                }
                if (context.testExpression("AttributesToGet", targetDepth)) {
                    request.setAttributesToGet(new ListUnmarshaller<String>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance()).unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static ScanRequestJsonUnmarshaller instance;
    public static ScanRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new ScanRequestJsonUnmarshaller();
        return instance;
    }
}
