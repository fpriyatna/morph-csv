package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.model.CSV;
import es.upm.fi.dia.oeg.model.CSVW;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RDBUtils {


    public String createTable(TriplesMap tripleMap, CSV csv, CSVW csvw){
        MappingUtils m = new MappingUtils();
        String[] headers = csv.getRows().get(0);
        CSVWUtils csvwUtils = new CSVWUtils();
        ArrayList<String> primaryKeys = m.getPrimaryKeys(tripleMap.getSubjectMap());
        String sourceUrl = ((Source)tripleMap.getLogicalSource()).getSourceName();
        HashMap<String,ArrayList<String>> foreignKeys = m.getForeignKeys(tripleMap.getPredicateObjectMaps(),headers);
        JSONArray annotations = csvwUtils.getAnnotationsFromSource(csvw.getContent().getJSONArray("tables"),sourceUrl);
        String tableName = sourceUrl.split("/")[sourceUrl.split("/").length-1].replace(".csv","").toUpperCase();
        String table="DROP TABLE IF EXISTS "+tableName+";\nCREATE TABLE "+tableName+" ";
        table+="(";

        for(String field : headers){
            if(checkColumnInMapping(field,tripleMap)) {
                for (Object o : annotations) {
                    String column = ((JSONObject) o).getString("title");
                    if (column.equals(field.trim())) {
                        JSONObject datatype = null;
                        if (((JSONObject) o).has("datatype"))
                            datatype = ((JSONObject) o).getJSONObject("datatype");
                        table += "`" + field.toUpperCase().trim() + "` " + getTypeForColumn(datatype) + ",";
                    }
                }
            }
        }

        table=table.substring(0,table.length()-1);
        if(!primaryKeys.isEmpty()) {
            table += ",PRIMARY KEY (";
            for (String p : primaryKeys) {
                table += p + ",";
            }
            table=table.substring(0,table.length()-1);
            table += ")";
        }

        if(!foreignKeys.isEmpty()) {
            for(Map.Entry<String, ArrayList<String>> entry : foreignKeys.entrySet()){
                table += ",FOREIGN KEY ("+entry.getValue().get(0)+") REFERENCES "+entry.getKey()+" ("+entry.getValue().get(1)+"),";

            }
            table=table.substring(0,table.length()-1);
        }


        table+=");\n";
        return table;
    }

    private String getTypeForColumn(JSONObject datatype){
        return CSVWUtils.fromBasetoDatatype(datatype.getString("base"));
    }

    private boolean checkColumnInMapping(String header, TriplesMap triplesMap){
        boolean flag = false;
        if(triplesMap.getSubjectMap().getTemplateString().matches(".*"+header+".*")){
            flag= true;
        }
        else {
            for(PredicateObjectMap pom : triplesMap.getPredicateObjectMaps()){
                for(ObjectMap ob : pom.getObjectMaps()){
                    if(ob.getColumn()!=null || ob.getTemplate()!=null){
                        if(ob.getColumn().matches(".*"+header+".*")){
                            flag = true;
                        }
                    }
                }
            }
        }

        return flag;
    }



}
