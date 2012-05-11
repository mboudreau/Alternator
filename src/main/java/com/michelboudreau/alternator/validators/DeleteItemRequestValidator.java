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

	public List<Error> validate(Object target) {
		DeleteItemRequest instance = (DeleteItemRequest) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
		errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), instance.getKey()));
		return removeNulls(errors);
	}
}
