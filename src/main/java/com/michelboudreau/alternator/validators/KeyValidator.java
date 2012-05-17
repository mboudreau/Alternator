package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.Key;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeyValidator extends Validator {

    public Boolean supports(Class clazz) {
        return Key.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        Key instance = (Key) target;
        List<Error> errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance);
        if (errors.size() == 0) {
            errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getHashKeyElement().toString());
        }
        return removeNulls(errors);
    }
}
