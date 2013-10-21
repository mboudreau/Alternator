package com.amazonaws.services.dynamodb.model.transform;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;

import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import com.fasterxml.jackson.core.JsonToken;

public class DescribeTableRequestJsonUnmarshaller implements Unmarshaller<DescribeTableRequest, JsonUnmarshallerContext> {

    public DescribeTableRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        DescribeTableRequest request = new DescribeTableRequest();

        int originalDepth = context.getCurrentDepth();
        int targetDepth = originalDepth + 1;

        JsonToken token = context.currentToken;
        if (token == null) token = context.nextToken();

        while (true) {
            if (token == null) break;

            if (token == FIELD_NAME || token == START_OBJECT) {
                if (token == JsonToken.FIELD_NAME || token == JsonToken.START_OBJECT) {
                    if (context.testExpression("TableName", targetDepth)) {
                        context.nextToken();
                        request.setTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
                    }
                }
            } else if (token == END_ARRAY || token == END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static DescribeTableRequestJsonUnmarshaller instance;
    public static DescribeTableRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new DescribeTableRequestJsonUnmarshaller();
        return instance;
    }
}
