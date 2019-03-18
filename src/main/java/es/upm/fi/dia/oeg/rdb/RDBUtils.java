package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.model.CSV;
import es.upm.fi.dia.oeg.model.CSVW;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RDBUtils {
    private String schema;


    public String createSQLSchema(Collection<TriplesMap> triplesMaps, ArrayList<CSV> csvs, CSVW csvw){
        schema = "";

        triplesMaps.forEach(triplesMap -> {
            for(CSV csv : csvs){
                if(csv.getUrl().equals(((Source)triplesMap.getLogicalSource()).getSourceName())){
                    schema += createTable(triplesMap,csv,csvs,csvw);
                    break;
                }
            }
        });
        System.out.println(schema);
        return schema;
    }

    public String createTable(TriplesMap tripleMap, CSV csv, ArrayList<CSV> csvs, CSVW csvw){
        MappingUtils m = new MappingUtils();
        String[] headers =null;
        if(csv.getRows()==null) {
              for(CSV c: csvs){
                  if(c.getUrl().equals(csv.getParentUrl())){
                      headers = c.getRows().get(0);
                  }
              }
        }
        else
            headers = csv.getRows().get(0);
        ArrayList<String> primaryKeys = m.getPrimaryKeys(tripleMap.getSubjectMap());
        String sourceUrl = ((Source)tripleMap.getLogicalSource()).getSourceName();
        HashMap<String,ArrayList<String>> foreignKeys = m.getForeignKeys(tripleMap);
        JSONArray annotations = CSVWUtils.getAnnotationsFromSource(csvw.getContent().getJSONArray("tables"),csv);
        String tableName = sourceUrl.split("/")[sourceUrl.split("/").length-1].replace(".csv","").toUpperCase();
        String table="DROP TABLE IF EXISTS "+tableName+";\nCREATE TABLE "+tableName+" ";
        table+="(";

        for(String field : headers){
            if(checkColumnInMapping(field,tripleMap)) {
                JSONObject datatype = null;
                Object def=null;
                for (Object o : annotations) {
                    String column = ((JSONObject) o).getString("titles");
                    if (column.equals(field.trim())) {
                        if (((JSONObject) o).has("datatype"))
                            datatype = ((JSONObject) o).getJSONObject("datatype");
                        if(((JSONObject) o).has("default")){
                            def = ((JSONObject) o).get("default");
                        }
                    }

                }
                if(datatype!=null)
                    table += "`" + field.toUpperCase().trim() + "` " + CSVWUtils.fromBasetoDatatype(datatype.getString("base"));
                else
                    table += "`" + field.toUpperCase().trim() + "` VARCHAR(200)";
                if(def==null)
                    table += ",";
                else
                    table += " DEFAULT "+def.toString()+",";
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


    private boolean checkColumnInMapping(String header, TriplesMap triplesMap){
        boolean flag = false;
        if(triplesMap.getSubjectMap().getTemplate().getColumnNames().contains(header.trim())){
            flag= true;
        }
        else {
            for(PredicateObjectMap pom : triplesMap.getPredicateObjectMaps()){
                for(ObjectMap ob : pom.getObjectMaps()){
                    if(ob.getColumn()!=null){
                        if(ob.getColumn().matches(".*"+header.trim()+".*")){
                            flag = true;
                        }
                    }
                    else if(ob.getTemplate()!=null){
                        if(ob.getTemplate().getColumnNames().contains(header.trim())){
                            flag = true;
                        }
                    }
                }
                for(RefObjectMap refObjectMap : pom.getRefObjectMaps()){
                    if(refObjectMap.getJoinCondition(0).getChild().matches(".*"+header.trim()+".*")){
                        flag= true;
                    }
                }
            }
        }

        return flag;
    }



}
