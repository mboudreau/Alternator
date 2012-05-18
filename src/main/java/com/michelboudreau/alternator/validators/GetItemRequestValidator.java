package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class GetItemRequestValidator extends Validator {

    public Boolean supports(Class clazz) {
        return GetItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        GetItemRequest instance = (GetItemRequest) target;
        List<Error> errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance);
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), instance.getKey()));
        return removeNulls(errors);
    }
}
