package com.amazonaws.services.dynamodb.model.transform;

import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import com.fasterxml.jackson.core.JsonToken;

public class DeleteItemRequestJsonUnmarshaller implements Unmarshaller<DeleteItemRequest, JsonUnmarshallerContext> {

	public DeleteItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
		DeleteItemRequest request = new DeleteItemRequest();

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
				if (context.testExpression("Key", targetDepth)) {
					request.setKey(KeyJsonUnmarshaller.getInstance().unmarshall(context));
				}
				if (context.testExpression("Expected", targetDepth)) {
					request.setExpected(new MapUnmarshaller<String, ExpectedAttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), ExpectedAttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
				}
				if (context.testExpression("ReturnValues", targetDepth)) {
					context.nextToken();
					request.setReturnValues(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
				}
			} else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
				if (context.getCurrentDepth() <= originalDepth) break;
			}
			token = context.nextToken();
		}

		return request;
	}

	private static DeleteItemRequestJsonUnmarshaller instance;

	public static DeleteItemRequestJsonUnmarshaller getInstance() {
		if (instance == null) instance = new DeleteItemRequestJsonUnmarshaller();
		return instance;
	}
}
