package com.michelboudreau.alternator.validators.element;

import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class TableNameValidator implements Validator {

	public boolean supports(Class clazz) {
		return String.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		String instance = (String) target;
        List<Error> errors = new ArrayList<Error>();

        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors, instance);
        errors = ValidatorUtils.rejectIfDoesntMatchRegex(errors,instance, "[a-zA-Z0-9_.-]");
        errors = ValidatorUtils.rejectIfSizeOutOfBounds(errors,instance, 3, 255);
		return errors;
	}
}
