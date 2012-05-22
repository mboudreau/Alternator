package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class PrimaryKeyValidator extends Validator {

	public Boolean supports(Class clazz) {
		return AttributeValue.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		AttributeValue instance = (AttributeValue) target;
		List<Error> errors = ValidatorUtils.rejectIfNull(instance);
		if (errors.size() == 0) {
			// Find value
			String value = null;
			if (instance.getN() != null) {
				value = instance.getN();
			} else if (instance.getS() != null) {
				value = instance.getS();
			}
			errors.addAll(ValidatorUtils.rejectIfNull(value));
			if(value != null) {
				errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(value, 1, 255));
			}

		}
		return removeNulls(errors);
	}
}
