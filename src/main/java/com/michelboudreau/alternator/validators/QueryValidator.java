package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class QueryValidator extends Validator {

    public Boolean supports(Class clazz) {
        return QueryRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        QueryRequest instance = (QueryRequest) target;
        List<Error> errors = new ArrayList<Error>();
        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getHashKeyValue().toString()));
        return removeNulls(errors);
    }
}
