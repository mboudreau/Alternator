
package com.michelboudreau.utils;


import com.michelboudreau.db.Item;
import org.springframework.core.convert.converter.Converter;
import org.springframework.format.FormatterRegistry;
import org.springframework.format.support.FormattingConversionServiceFactoryBean;

public class ConversionServiceFactory extends FormattingConversionServiceFactoryBean {

    
    public ConversionServiceFactory() {
    }

    public Converter<Item, String> getItemToStringConverter() {
        return new Converter<Item, String>() {

            @Override
            public String convert(Item item) {
                return new StringBuilder().append(item.getAttributes().get(item.getHashKey())).toString();
            }
        };
    }



    public void installConverters(FormatterRegistry registry) {
        registry.addConverter(getItemToStringConverter());
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        installConverters(getObject());
    }
}
