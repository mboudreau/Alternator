package com.amazonaws.services.dynamodb.model.transform;

import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.ListUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import com.fasterxml.jackson.core.JsonToken;


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
	             if (context.testExpression("Count", targetDepth)) {
                    context.nextToken();
                    request.setCount(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
	            if (context.testExpression("ConsistentRead", targetDepth)) {
                    context.nextToken();
                    request.setConsistentRead(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
	             if (context.testExpression("ScanIndexForward", targetDepth)) {
                    context.nextToken();
                    request.setScanIndexForward(SimpleTypeJsonUnmarshallers.BooleanJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("HashKeyValue", targetDepth)) {
                    request.setHashKeyValue(AttributeValueJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("RangeKeyCondition", targetDepth)) {
                    request.setRangeKeyCondition(new ConditionJsonUnmarshaller().unmarshall(context));
                }
                if (context.testExpression("ExclusiveStartKey", targetDepth)) {
                    request.setExclusiveStartKey(KeyJsonUnmarshaller.getInstance().unmarshall(context));
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
