package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class ScanRequestValidator extends Validator {

    public Boolean supports(Class clazz) {
        return ScanRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        ScanRequest instance = (ScanRequest) target;
	    List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
        return removeNulls(errors);
    }
}
