package com.michelboudreau.alternator.models;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ResourceNotFoundException;
import com.michelboudreau.alternator.enums.AttributeValueType;
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

    public Collection<Map<String, AttributeValue>> getItems(
            KeySchemaElement rangeKeyElement,
            Condition rangeKeyCondition) {

        if ((rangeKeyElement == null) || (rangeKeyCondition == null)) {
            return items.values();
        } else {
            return filterItemsByRangeKeyCondition(rangeKeyElement, rangeKeyCondition);
        }
    }

    private AttributeValueType getAttributeValueType(AttributeValue value) {
		if (value != null) {
			if (value.getN() != null) {
				return AttributeValueType.N;
			} else if (value.getS() != null) {
				return AttributeValueType.S;
			} else if (value.getNS() != null) {
				return AttributeValueType.NS;
			} else if (value.getSS() != null) {
				return AttributeValueType.SS;
			}
		}
		return AttributeValueType.UNKNOWN;
	}

    private Collection<Map<String, AttributeValue>> filterItemsByRangeKeyCondition(
            KeySchemaElement rangeKeyElement,
            Condition cond) {

        String rangeKeyName = rangeKeyElement.getAttributeName();
        Collection<Map<String, AttributeValue>> filteredItems = new ArrayList<Map<String, AttributeValue>>();

        for (String rangeKey : items.keySet()) {
            Map<String, AttributeValue> item = items.get(rangeKey);

            if (cond.getComparisonOperator() == null) {
                throw new ResourceNotFoundException("There must be a comparisonOperator");
            }
            else if (cond.getComparisonOperator().equals("EQ")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.compareTo(comp) == 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("LE")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.compareTo(comp) <= 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("LT")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.compareTo(comp) < 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("GE")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.compareTo(comp) >= 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("GT")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.compareTo(comp) > 0) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("BETWEEN")) {
                if (cond.getAttributeValueList().size() == 2) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr0 = cond.getAttributeValueList().get(0);
                    String comp0 = (getAttributeValueType(compAttr0).equals(AttributeValueType.S)) ? compAttr0.getS() : compAttr0.getN();
                    AttributeValue compAttr1 = cond.getAttributeValueList().get(1);
                    String comp1 = (getAttributeValueType(compAttr1).equals(AttributeValueType.S)) ? compAttr1.getS() : compAttr1.getN();
                    if ((value.compareTo(comp0) >= 0) && (value.compareTo(comp1) <= 0)) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("BEGINS_WITH")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.startsWith(comp)) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("CONTAINS")) {
                if (cond.getAttributeValueList().size() == 1) {
                    AttributeValue valueAttr = item.get(rangeKeyName);
                    String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                    AttributeValue compAttr = cond.getAttributeValueList().get(0);
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.contains(comp)) {
                        filteredItems.add(item);
                    }
                }
            }
            else if (cond.getComparisonOperator().equals("IN")) {
                AttributeValue valueAttr = item.get(rangeKeyName);
                String value = (getAttributeValueType(valueAttr).equals(AttributeValueType.S)) ? valueAttr.getS() : valueAttr.getN();
                for(AttributeValue compAttr : cond.getAttributeValueList()){
                    String comp = (getAttributeValueType(compAttr).equals(AttributeValueType.S)) ? compAttr.getS() : compAttr.getN();
                    if (value.compareTo(comp) == 0) {
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
