package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class KeySchemaElementValidator extends Validator {

	public Boolean supports(Class clazz) {
		return KeySchemaElement.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		KeySchemaElement instance = (KeySchemaElement) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new AttributeNameValidator(), instance.getAttributeName());
		errors.addAll(ValidatorUtils.invokeValidator(new AttributeTypeValidator(), instance.getAttributeType()));

		return removeNulls(errors);
	}
}
