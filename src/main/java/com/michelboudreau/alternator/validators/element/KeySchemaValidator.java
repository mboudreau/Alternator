package com.michelboudreau.alternator.validators.element;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeySchemaValidator implements Validator {

    public boolean supports(Class clazz) {
        return KeySchema.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        KeySchema instance = (KeySchema) target;
        List<Error> errors = new ArrayList<Error>();

        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors, instance.toString());
        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors, instance.getHashKeyElement().toString());
        errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getHashKeyElement()));

        if (instance.getRangeKeyElement() != null) {
            errors.addAll(ValidatorUtils.invokeValidator(new KeySchemaElementValidator(), instance.getRangeKeyElement()));

        }
        return errors;
    }
}
