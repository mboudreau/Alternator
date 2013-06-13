package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.ListUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import com.amazonaws.transform.MapUnmarshaller;
import org.codehaus.jackson.JsonToken;

public class QueryRequestJsonUnmarshaller implements Unmarshaller<QueryRequest, JsonUnmarshallerContext> {

    public QueryRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        QueryRequest request = new QueryRequest();

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
                }
	            if (context.testExpression("Select", targetDepth)) {
                    context.nextToken();
                    request.setSelect(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
	            if (context.testExpression("ConsistentRead", targetDepth)) {
                    context.nextToken();
                    request.setConsistentRead(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
	             if (context.testExpression("ScanIndexForward", targetDepth)) {
                    context.nextToken();
                    request.setScanIndexForward(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("KeyConditions", targetDepth)) {
                    request.setKeyConditions(new MapUnmarshaller<String, Condition>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), ConditionJsonUnmarshaller.getInstance()).unmarshall(context));
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

    private static QueryRequestJsonUnmarshaller instance;

    public static QueryRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new QueryRequestJsonUnmarshaller();
        return instance;
    }
}
