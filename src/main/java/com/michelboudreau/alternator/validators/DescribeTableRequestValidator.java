package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class DescribeTableRequestValidator implements Validator {

    public boolean supports(Class clazz) {
        return DescribeTableRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        DescribeTableRequest instance = (DescribeTableRequest) target;
        List<Error> errors = new ArrayList<Error>();
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        return errors;
    }
}
