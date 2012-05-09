package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.KeySchemaElementValidator;
import com.michelboudreau.alternator.validators.element.KeyValidator;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class QueryValidator implements Validator {

    public boolean supports(Class clazz) {
        return QueryRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        QueryRequest instance = (QueryRequest) target;
        List<Error> errors = new ArrayList<Error>();

        errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName()));
        errors = ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(errors, instance.getHashKeyValue().toString());

        return errors;
    }
}
