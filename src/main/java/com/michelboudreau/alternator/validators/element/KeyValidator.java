package com.michelboudreau.alternator.validators.element;

import com.amazonaws.services.dynamodb.model.Key;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeyValidator implements Validator {

	public boolean supports(Class clazz) {
		return Key.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		Key instance = (Key) target;
        List<Error> errors = new ArrayList<Error>();
        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors,instance.getHashKeyElement().toString());
		return errors;
	}
}
