package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

public class ListTablesRequestJsonUnmarshaller implements Unmarshaller<ListTablesRequest, JsonUnmarshallerContext> {

    public ListTablesRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        ListTablesRequest request = new ListTablesRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

        while (true) {
            if (token == null) break;

            if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                if (context.testExpression("ExclusiveStartTableName", targetDepth)) {
                    context.nextToken();
                    request.setExclusiveStartTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                }
                if (context.testExpression("Limit", targetDepth)) {
                    context.nextToken();
                    request.setLimit(SimpleTypeJsonUnmarshallers.IntegerJsonUnmarshaller.getInstance().unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static ListTablesRequestJsonUnmarshaller instance;

    public static ListTablesRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new ListTablesRequestJsonUnmarshaller();
        return instance;
    }
}
