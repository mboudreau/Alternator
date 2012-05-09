package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.KeySchemaValidator;
import com.michelboudreau.alternator.validators.element.ProvisionedThroughputValidator;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class UpdateTableRequestValidator implements Validator {

    public boolean supports(Class clazz) {
        return UpdateTableRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        UpdateTableRequest instance = (UpdateTableRequest) target;
        List<Error> errors = new ArrayList<Error>();
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        errors.addAll(ValidatorUtils.invokeValidator(new ProvisionedThroughputValidator(), instance.getProvisionedThroughput()));
        return errors;
    }
}
