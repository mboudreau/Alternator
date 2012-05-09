package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

public class KeySchemaElementValidator implements Validator {

	public boolean supports(Class clazz) {
		return KeySchemaElement.class.isAssignableFrom(clazz);
	}

	public Error[] validate(Object target) {
		KeySchemaElement instance = (KeySchemaElement) target;

		ValidatorUtils.rejectIfNullOrEmptyOrWhitespace("attributeName");
		ValidatorUtils.rejectIfNullOrEmptyOrWhitespace("attributeType");

		/*if (!errors.hasFieldErrors("attributeName")) {
			if (instance.getAttributeName().length() > 255)
				errors.rejectValue("attributeName", "too_long", "Length cannot exceed 255");
		}

		if (!errors.hasFieldErrors("attributeType")) {
			if (instance.getAttributeType() != "S" && instance.getAttributeType() != "N")
				errors.rejectValue("attributeType", "wrong_type", "Must be of type 'S' or 'N'");
		}*/
		return null;
	}
}
