package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeySchemaValidator extends Validator {

    public Boolean supports(Class clazz) {
        return KeySchema.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        KeySchema instance = (KeySchema) target;
        List<Error> errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.toString());
        errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getHashKeyElement().toString()));
        errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getHashKeyElement()));

        if (instance.getRangeKeyElement() != null) {
            errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getRangeKeyElement()));

        }
        return removeNulls(errors);
    }
}
