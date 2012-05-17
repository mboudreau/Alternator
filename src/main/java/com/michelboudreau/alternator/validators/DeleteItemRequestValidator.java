package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class DeleteItemRequestValidator extends Validator {

	public Boolean supports(Class clazz) {
		return DeleteItemRequest.class.isAssignableFrom(clazz);
	}

	/*
	{"TableName":"Table1",
    "Key":
        {"HashKeyElement":{"S":"AttributeValue1"},"RangeKeyElement":{"N":"AttributeValue2"}},
    "Expected":{"AttributeName3":{"Value":{"S":"AttributeValue3"}}},
    "ReturnValues":"ALL_OLD"}
}
	 */

	public List<Error> validate(Object target) {
		DeleteItemRequest instance = (DeleteItemRequest) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
		errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), instance.getKey()));
		if (instance.getExpected() != null) {
			errors.addAll(ValidatorUtils.invokeValidator(new ExpectedValidator(), instance.getExpected()));
		}
		if (instance.getReturnValues() != null) {
			errors.addAll(ValidatorUtils.rejectIfNotMatchRegex(instance.getReturnValues(), "^(NONE|ALL_OLD)$"));
		}
		return removeNulls(errors);
	}
}
