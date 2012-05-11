package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class UpdateTableRequestValidator extends Validator {

    public Boolean supports(Class clazz) {
        return UpdateTableRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        UpdateTableRequest instance = (UpdateTableRequest) target;
        List<Error> errors = new ArrayList<Error>();
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        errors.addAll(ValidatorUtils.invokeValidator(new ProvisionedThroughputValidator(), instance.getProvisionedThroughput()));
        return removeNulls(errors);
    }
}
