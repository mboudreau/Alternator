package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class PutItemRequestValidator extends Validator {

	public Boolean supports(Class clazz) {
		return PutItemRequest.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		PutItemRequest instance = (PutItemRequest) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
		errors.addAll(ValidatorUtils.invokeValidator(new ItemValidator(), instance.getItem()));
		if (instance.getExpected() != null) {
			errors.addAll(ValidatorUtils.invokeValidator(new ExpectedValidator(), instance.getExpected()));
		}
		if (instance.getReturnValues() != null) {
			errors.addAll(ValidatorUtils.rejectIfNotMatchRegex(instance.getReturnValues(), "^(NONE|ALL_OLD)$"));
		}
		return removeNulls(errors);
	}
}
