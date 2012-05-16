package com.michelboudreau.alternator.validators;

import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableNameValidator extends Validator {

	public Boolean supports(Class clazz) {
		return String.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		List<Error> errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(target);
		if(errors.size() == 0) {
			errors.addAll(ValidatorUtils.rejectIfNotMatchRegex(target, "^[a-zA-Z0-9_.-]*$"));
			errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(target, 3, 255));
		}
		return removeNulls(errors);
	}
}
