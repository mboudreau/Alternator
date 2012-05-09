package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.KeyValidator;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class UpdateItemRequestValidator implements Validator {

    public boolean supports(Class clazz) {
        return UpdateItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        UpdateItemRequest instance = (UpdateItemRequest) target;
        List<Error> errors = new ArrayList<Error>();

        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));

        errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), instance.getKey()));
        errors = (ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors, instance.getAttributeUpdates().toString()));

        return errors;
    }
}
