package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.ResourceNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.codehaus.jackson.annotate.JsonIgnore;

public class ItemRangeGroup {
    
    @JsonIgnore()
    private final String DEFAULT_RANGE_KEY_VALUE = "";

    private SortedMap<String, Map<String, AttributeValue>> items = new TreeMap<String, Map<String, AttributeValue>>();

    public int size() {
        return items.size();
    }

    public Set<String> getKeySet() {
        return items.keySet();
    }

    public Collection<Map<String, AttributeValue>> getItems(Condition rangeKeyCondition) {
        if (rangeKeyCondition == null) {
            return items.values();
        } else {
            return filterItemsByRangeKeyCondition(rangeKeyCondition);
        }
    }

    private Collection<Map<String, AttributeValue>> filterItemsByRangeKeyCondition(Condition cond) {
        Collection<Map<String, AttributeValue>> filteredItems = new ArrayList<Map<String, AttributeValue>>();

        for (String rangeKey : items.keySet()) {
            Map<String, AttributeValue> item = items.get(rangeKey);

            if (cond.getComparisonOperator() == null) {
                throw new ResourceNotFoundException("There must be a comparisonOperator");
            }
            else if (cond.getComparisonOperator().equals("EQ")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.equals(cond.getAttributeValueList().get(0).getS())) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("LE")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.compareTo(cond.getAttributeValueList().get(0).getS()) <= 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("LT")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.compareTo(cond.getAttributeValueList().get(0).getS()) < 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("GE")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.compareTo(cond.getAttributeValueList().get(0).getS()) >= 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("GT")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.compareTo(cond.getAttributeValueList().get(0).getS()) > 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("BETWEEN")) {
                if (cond.getAttributeValueList().size() == 2) {
                    if ((rangeKey.compareTo(cond.getAttributeValueList().get(0).getS()) >= 0) &&
                        (rangeKey.compareTo(cond.getAttributeValueList().get(1).getS()) <= 0)) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("BEGINS_WITH")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.startsWith(cond.getAttributeValueList().get(0).getS())) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("CONTAINS")) {
                if (cond.getAttributeValueList().size() == 1) {
                    if (rangeKey.contains(cond.getAttributeValueList().get(0).getS())) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("IN")) {
                for(AttributeValue value : cond.getAttributeValueList()){
                    if(rangeKey.equals(value.getS())){
                        filteredItems.add(item);
                        break; // out of 'for' loop
                    }
                }
            }
        }

        return filteredItems;
    }

	public void putItem(String rangeKeyValue, Map<String, AttributeValue> item) {
        items.put(normalizeRangeKeyValue(rangeKeyValue), item);
	}

	public void removeItem(String rangeKeyValue) {
        items.remove(normalizeRangeKeyValue(rangeKeyValue));
	}

	public Map<String, AttributeValue> getItem(String rangeKeyValue) {
        return items.get(normalizeRangeKeyValue(rangeKeyValue));
	}

    private String normalizeRangeKeyValue(String rangeKeyValue) {
        String actualRangeKeyValue = rangeKeyValue;
        if (rangeKeyValue == null) {
            actualRangeKeyValue = DEFAULT_RANGE_KEY_VALUE;
        }
        return actualRangeKeyValue;
    }
}
