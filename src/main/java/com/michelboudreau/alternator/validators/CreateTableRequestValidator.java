package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class CreateTableRequestValidator extends Validator {

	public Boolean supports(Class clazz) {
		return CreateTableRequest.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		CreateTableRequest instance = (CreateTableRequest) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
		errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaValidator(), instance.getKeySchema()));
		errors.addAll(ValidatorUtils.invokeValidator(new ProvisionedThroughputValidator(), instance.getProvisionedThroughput()));
		return removeNulls(errors);
	}
}
