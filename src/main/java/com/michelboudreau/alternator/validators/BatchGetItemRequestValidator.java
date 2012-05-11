package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class BatchGetItemRequestValidator extends Validator {

    public Boolean supports(Class clazz) {
        return BatchGetItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        BatchGetItemRequest instance = (BatchGetItemRequest) target;
        List<Error> errors = new ArrayList<Error>();
	    //TODO: Redo this logic, look at whole request object
        for (String tablename : instance.getRequestItems().keySet()) {
            errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), tablename));
            errors.addAll(ValidatorUtils.invokeValidator(new KeysAndAttributesValidator(), instance.getRequestItems().get(tablename)));
        }
        return removeNulls(errors);
    }
}
