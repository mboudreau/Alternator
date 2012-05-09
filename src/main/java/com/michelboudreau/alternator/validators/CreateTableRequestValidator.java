package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

public class CreateTableRequestValidator implements Validator {

	public boolean supports(Class clazz) {
		return CreateTableRequest.class.isAssignableFrom(clazz);
	}

	public Error[] validate(Object target) {
		CreateTableRequest instance = (CreateTableRequest) target;

		ValidatorUtils.rejectIfNullOrEmptyOrWhitespace("tableName");
		ValidatorUtils.invokeValidator(new KeySchemaValidator(), instance.getKeySchema());
		return null;
	}
}
