package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.Key;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class KeyValidator extends Validator {

    public Boolean supports(Class clazz) {
        return Key.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        Key instance = (Key) target;
        List<Error> errors = ValidatorUtils.rejectIfNull(instance);
        if (errors.size() == 0) {
            errors.addAll(ValidatorUtils.rejectIfNull(instance.getHashKeyElement()));
	        if(instance.getHashKeyElement() != null) {
		        errors.addAll(ValidatorUtils.invokeValidator(new PrimaryKeyValidator(), instance.getHashKeyElement()));
	        }
	        if(instance.getRangeKeyElement() != null) {
			 errors.addAll(ValidatorUtils.invokeValidator(new PrimaryKeyValidator(), instance.getRangeKeyElement()));
	        }
        }
        return removeNulls(errors);
    }
}
