package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.michelboudreau.alternator.models.Limits;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class ProvisionedThroughputValidator extends Validator {

	public Boolean supports(Class clazz) {
		return ProvisionedThroughput.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		ProvisionedThroughput instance = (ProvisionedThroughput) target;
		List<Error> errors = ValidatorUtils.rejectIfNull(instance);
		if (errors.size() == 0) {
			errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(instance.getWriteCapacityUnits(), 1, Limits.NUMBER_MAX));
			errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(instance.getReadCapacityUnits(), 1, Limits.NUMBER_MAX));
		}

		return removeNulls(errors);
	}
}
