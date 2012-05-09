package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.KeyValidator;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class DeleteItemRequestValidator implements Validator {

    public boolean supports(Class clazz) {
        return DeleteItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        DeleteItemRequest instance = (DeleteItemRequest) target;
        List<Error> errors = new ArrayList<Error>();

        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));

        errors.addAll(ValidatorUtils.invokeValidator(new KeyValidator(), instance.getKey())) ;
        return errors;
    }
}
