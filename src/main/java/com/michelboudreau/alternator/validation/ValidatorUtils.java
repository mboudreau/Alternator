package com.michelboudreau.alternator.validation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidatorUtils {
	private static Log logger = LogFactory.getLog(ValidatorUtils.class);

	public static List<Error> invokeValidator(Validator validator, Object obj) {
		Assert.notNull(validator, "Validator must not be null");
		if (logger.isDebugEnabled()) {
			logger.debug("Invoking validator [" + validator + "]");
		}
		if (obj != null && !validator.supports(obj.getClass())) {
			throw new IllegalArgumentException("Validator [" + validator.getClass() + "] does not support [" + obj.getClass() + "]");
		} else {
			return validator.validate(obj);
		}
	}

	public static List<Error> rejectIfNullOrEmptyOrWhitespace(List<Error> errors,String property) {
        if(property==null) {
            errors.add(new Error("property value is null or empty"));
        }else if (property.replaceAll(" ","").equals("")){
            errors.add(new Error("property value is null or empty"));
        }
        return errors;
	}

    public static List<Error> rejectIfDoesntMatchRegex(List<Error> errors,String property,String regex) {
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(property);
        if (!m.matches()  ) {
            errors.add(new Error("property doesn't match provided regex : " +regex));
        }
        return errors;
    }

    public static List<Error> rejectIfSizeOutOfBounds(List<Error> errors, String property,int min,int max) {
        if (!(property.length() <= max) || !(property.length() >= min)) {
            errors.add(new Error("property is out of provided bounds."));
        }
        return errors;
    }
}
