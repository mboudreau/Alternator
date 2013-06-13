package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.MapUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

import static org.codehaus.jackson.JsonToken.*;

public class PutItemRequestJsonUnmarshaller implements Unmarshaller<PutItemRequest, JsonUnmarshallerContext> {

	public PutItemRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
		PutItemRequest request = new PutItemRequest();

		int originalDepth = context.getCurrentDepth();
		int targetDepth = originalDepth + 1;

		JsonToken token = context.currentToken;
		if (token == null) token = context.nextToken();

		while (true) {
			if (token == null) break;

			if (token == FIELD_NAME || token == START_OBJECT) {
				if (context.testExpression("TableName", targetDepth)) {
					context.nextToken();
					request.setTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
				}
				if (context.testExpression("Item", targetDepth)) {
					request.setItem(new MapUnmarshaller<String, AttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), AttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
				}
				if (context.testExpression("Expected", targetDepth)) {
					request.setExpected(new MapUnmarshaller<String, ExpectedAttributeValue>(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance(), ExpectedAttributeValueJsonUnmarshaller.getInstance()).unmarshall(context));
				}
				if (context.testExpression("ReturnValues", targetDepth)) {
					context.nextToken();
					request.setReturnValues(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
				}
			} else if (token == END_ARRAY || token == END_OBJECT) {
				if (context.getCurrentDepth() <= originalDepth) break;
			}
			token = context.nextToken();
		}

		return request;
	}

	private static PutItemRequestJsonUnmarshaller instance;

	public static PutItemRequestJsonUnmarshaller getInstance() {
		if (instance == null) instance = new PutItemRequestJsonUnmarshaller();
		return instance;
	}
}
