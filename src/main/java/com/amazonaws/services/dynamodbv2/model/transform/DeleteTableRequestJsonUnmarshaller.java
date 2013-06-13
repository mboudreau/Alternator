package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

public class DeleteTableRequestJsonUnmarshaller implements Unmarshaller<DeleteTableRequest, JsonUnmarshallerContext> {

    public DeleteTableRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        DeleteTableRequest request = new DeleteTableRequest();

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
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static DeleteTableRequestJsonUnmarshaller instance;
    public static DeleteTableRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new DeleteTableRequestJsonUnmarshaller();
        return instance;
    }
}
