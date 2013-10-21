package com.michelboudreau.alternator.validators;

import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class AttributeNameValidator extends Validator {

	public Boolean supports(Class clazz) {
		return String.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		String instance = (String) target;
		List<Error> errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance);
		errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(instance, 1, 255));
		return removeNulls(errors);
	}
}
