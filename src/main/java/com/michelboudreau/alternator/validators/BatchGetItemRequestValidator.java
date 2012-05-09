package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;
import com.michelboudreau.alternator.validators.element.KeysAndAttributesValidator;
import com.michelboudreau.alternator.validators.element.ProvisionedThroughputValidator;
import com.michelboudreau.alternator.validators.element.TableNameValidator;

import java.util.ArrayList;
import java.util.List;

public class BatchGetItemRequestValidator implements Validator {

    public boolean supports(Class clazz) {
        return BatchGetItemRequest.class.isAssignableFrom(clazz);
    }

    public List<Error> validate(Object target) {
        BatchGetItemRequest instance = (BatchGetItemRequest) target;
        List<Error> errors = new ArrayList<Error>();
        for (String tablename : instance.getRequestItems().keySet()) {
            errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), tablename));
            errors.addAll(ValidatorUtils.invokeValidator(new KeysAndAttributesValidator(), instance.getRequestItems().get(tablename)));
        }
        return errors;
    }
}
