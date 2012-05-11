package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class ProvisionedThroughputValidator extends Validator {

	public Boolean supports(Class clazz) {
		return ProvisionedThroughput.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		ProvisionedThroughput instance = (ProvisionedThroughput) target;
		List<Error> errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.toString());
		errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getReadCapacityUnits().toString()));
		errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getWriteCapacityUnits().toString()));

		return removeNulls(errors);
	}
}
