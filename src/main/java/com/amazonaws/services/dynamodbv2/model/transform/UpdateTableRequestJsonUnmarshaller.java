package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

public class UpdateTableRequestJsonUnmarshaller implements Unmarshaller<UpdateTableRequest, JsonUnmarshallerContext> {

    public UpdateTableRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
        UpdateTableRequest request = new UpdateTableRequest();

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
	            if (context.testExpression("ProvisionedThroughput", targetDepth)) {
	                request.setProvisionedThroughput(ProvisionedThroughputJsonUnmarshaller.getInstance().unmarshall(context));
                }
            } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                if (context.getCurrentDepth() <= originalDepth) break;
            }
            token = context.nextToken();
        }

        return request;
    }

    private static UpdateTableRequestJsonUnmarshaller instance;
    public static UpdateTableRequestJsonUnmarshaller getInstance() {
        if (instance == null) instance = new UpdateTableRequestJsonUnmarshaller();
        return instance;
    }
}
