package com.michelboudreau.alternator.validators.element;

import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.amazonaws.services.dynamodb.model.Key;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeysAndAttributesValidator implements Validator {

    public boolean supports(Class clazz) {
        return KeysAndAttributes.class.isAssignableFrom(clazz);

    }

    public List<Error> validate(Object target) {
        KeysAndAttributes instance = (KeysAndAttributes) target;
        List<Error> errors = new ArrayList<Error>();
        for (Key key : instance.getKeys()) {
            errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), key));

        }
        return errors;
    }
}
