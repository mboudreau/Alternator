package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class KeysAndAttributesValidator extends Validator {

    public Boolean supports(Class clazz) {
        return KeysAndAttributes.class.isAssignableFrom(clazz);

    }

    public List<Error> validate(Object target) {
        KeysAndAttributes instance = (KeysAndAttributes) target;
        List<Error> errors = new ArrayList<Error>();
	    // TODO: Make sure this works
        for (Key key : instance.getKeys()) {
            errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), key));
        }
        return removeNulls(errors);
    }
}
