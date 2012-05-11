package com.michelboudreau.alternator.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Validator {
	public abstract Boolean supports(Class clazz);
    public abstract List<Error> validate(Object target);

	protected List<Error> removeNulls(List<Error> errors) {
		if(errors.size() != 0) {
			errors.removeAll(Collections.singletonList(null));
		}
		return errors;
	}
}
