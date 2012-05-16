package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class KeySchemaValidator extends Validator {

	public Boolean supports(Class clazz) {
		return KeySchema.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		KeySchema instance = (KeySchema) target;
		List<Error> errors = ValidatorUtils.rejectIfNull(instance);
		if (errors.size() == 0) {
			errors.addAll(ValidatorUtils.rejectIfNull(instance.getHashKeyElement()));
			if (errors.size() == 0) {
				if (instance.getHashKeyElement() != null) {
					errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getHashKeyElement()));
				}

				if (instance.getRangeKeyElement() != null) {
					errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getRangeKeyElement()));
					if (instance.getHashKeyElement().getAttributeName().equals(instance.getRangeKeyElement().getAttributeName())) {
						errors.add(new Error("Both the Hash Key and the Range Key element in the KeySchema have the same name."));
					}
				}
			}
		}
		return removeNulls(errors);
	}
}
