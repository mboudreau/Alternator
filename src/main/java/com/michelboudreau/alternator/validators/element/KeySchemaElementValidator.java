package com.michelboudreau.alternator.validators.element;

import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeySchemaElementValidator implements Validator {

	public boolean supports(Class clazz) {
		return KeySchemaElement.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		KeySchemaElement instance = (KeySchemaElement) target;
        List<Error> errors = new ArrayList<Error>();

        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors,instance.getAttributeName().toString());
        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors,instance.getAttributeType().toString());
        errors = ValidatorUtils.rejectIfSizeOutOfBounds(errors,instance.getAttributeName().toString(), 1, 255);

		return errors;
	}
}
