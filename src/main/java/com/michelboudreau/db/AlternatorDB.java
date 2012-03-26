package com.michelboudreau.db;



import java.util.ArrayList;
import java.util.List;


public class AlternatorDB {
	private List<Table> tables;

    public AlternatorDB() {
        tables = new ArrayList<Table>();
    }

    public void addTable(Table table) {
        tables.add(table);
    }
    
    public Element getElementFromTable(String tableName, String hashKey){
        Element el =new Element(null,null,null,null);
        for (Table tbl : tables){
            if(tableName.equals(tbl.getName())){
                for(Element elem : tbl.getElements()){
                    el = (hashKey.equals(el.getHashKey()))? elem : el;
                }
            }
        }
        return el;
    }
    
    public boolean putElementInTable(String tableName, String hashKey,Element el){
        boolean result = false;
        for (Table tbl : tables){
            if(tableName.equals(tbl.getName())){
                List<Element> elements = tbl.getElements();
                elements.add(el);
                tbl.setElements(elements);
            }
        }
        return result;
    }
}
