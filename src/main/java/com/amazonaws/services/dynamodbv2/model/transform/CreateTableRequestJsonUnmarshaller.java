package com.amazonaws.services.dynamodbv2.model.transform;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.ListUnmarshaller;
import com.amazonaws.transform.SimpleTypeJsonUnmarshallers;
import com.amazonaws.transform.Unmarshaller;
import org.codehaus.jackson.JsonToken;

import static org.codehaus.jackson.JsonToken.*;

public class CreateTableRequestJsonUnmarshaller implements Unmarshaller<CreateTableRequest, JsonUnmarshallerContext> {

	public CreateTableRequest unmarshall(JsonUnmarshallerContext context) throws Exception {
		CreateTableRequest request = new CreateTableRequest();

		int originalDepth = context.getCurrentDepth();
		int targetDepth = originalDepth + 1;

		JsonToken token = context.currentToken;
		if (token == null) token = context.nextToken();

		while (true) {
			if (token == null) break;

			if (token == FIELD_NAME /*|| token == START_OBJECT*/) {
				if (context.testExpression("AttributeDefinitions", targetDepth)) {
                    request.setAttributeDefinitions(new ListUnmarshaller<AttributeDefinition>(AttributeDefinitionJsonUnmarshaller.getInstance()).unmarshall(context));
				}
				if (context.testExpression("TableName", targetDepth)) {
					context.nextToken();
					request.setTableName(SimpleTypeJsonUnmarshallers.StringJsonUnmarshaller.getInstance().unmarshall(context));
				}
				if (context.testExpression("KeySchema", targetDepth)) {
                    request.setKeySchema(new ListUnmarshaller<KeySchemaElement>(KeySchemaElementJsonUnmarshaller.getInstance()).unmarshall(context));
				}
                if (context.testExpression("ProvisionedThroughput", targetDepth)) {
	                request.setProvisionedThroughput(ProvisionedThroughputJsonUnmarshaller.getInstance().unmarshall(context));
                }
			} else if (token == END_ARRAY || token == END_OBJECT) {
				if (context.getCurrentDepth() <= originalDepth) break;
			}
			token = context.nextToken();
		}

		return request;
	}

	private static CreateTableRequestJsonUnmarshaller instance;

	public static CreateTableRequestJsonUnmarshaller getInstance() {
		if (instance == null) instance = new CreateTableRequestJsonUnmarshaller();
		return instance;
	}
}
