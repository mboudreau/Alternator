package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import org.springframework.validation.Errors;

public class KeySchemaValidator implements Validator {

	public boolean supports(Class clazz) {
		return KeySchema.class.isAssignableFrom(clazz);
	}

	public Error[] validate(Object target) {
		KeySchema instance = (KeySchema) target;
		ValidatorUtils.rejectIfNullOrEmptyOrWhitespace("hashKeyElement");

		if (instance.getHashKeyElement() != null) {
			ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getHashKeyElement());
		}

		if (instance.getRangeKeyElement() != null) {
			ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getRangeKeyElement());
		}
		return null;
	}
}
