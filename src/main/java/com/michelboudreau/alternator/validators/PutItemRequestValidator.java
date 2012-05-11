package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class PutItemRequestValidator extends Validator {

    public Boolean supports(Class clazz) {
        return PutItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        PutItemRequest instance = (PutItemRequest) target;
        List<Error> errors = new ArrayList<Error>();
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName())) ;
        errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getItem().toString()));
        return removeNulls(errors);
    }
}
