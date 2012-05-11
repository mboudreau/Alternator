package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class UpdateItemRequestValidator extends Validator {

    public Boolean supports(Class clazz) {
        return UpdateItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        UpdateItemRequest instance = (UpdateItemRequest) target;
	    List<Error> errors = new ArrayList<Error>();
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), instance.getKey()));
        errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getAttributeUpdates().toString()));
        return removeNulls(errors);
    }
}
