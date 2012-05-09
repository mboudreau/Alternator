package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class ScanValidator implements Validator {

    public boolean supports(Class clazz) {
        return ScanRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        ScanRequest instance = (ScanRequest) target;
        List<Error> errors = new ArrayList<Error>();

        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        return errors;
    }
}
