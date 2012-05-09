package com.michelboudreau.alternator.validators.element;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class ProvisionedThroughputValidator implements Validator {

    public boolean supports(Class clazz) {
        return ProvisionedThroughput.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        ProvisionedThroughput instance = (ProvisionedThroughput) target;
        List<Error> errors = new ArrayList<Error>();

        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors,instance.toString());
        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors,instance.getReadCapacityUnits().toString());
        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors,instance.getWriteCapacityUnits().toString());
        return errors;
    }
}
