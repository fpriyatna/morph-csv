package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.rmlc.api.model.ObjectMap;
import org.json.JSONArray;
import org.json.JSONObject;

public class CSVWUtils {



    public static String fromBasetoDatatype(String base){
        String rdbDatatype="VARCHAR(200)";
        switch (base){
            case "date":
                rdbDatatype = "DATE";
                break;
            case "integer":
                rdbDatatype = "INT";
                break;
            case "double":
                rdbDatatype = "DOUBLE";
                break;
            case "boolean":
                rdbDatatype = "BOOLEAN";
                break;
            case "float":
                rdbDatatype = "FLOAT";
                break;
            case "datatetime":
                rdbDatatype = "TIMESTAMP";
                break;

        }
        return rdbDatatype;
    }

    public JSONArray getAnnotationsFromSource (JSONArray tables, String urlSource){
        JSONArray annotations = null;
        for(Object o : tables){
            annotations = ((JSONObject) o).getJSONObject("tableSchema").getJSONArray("columns");
            String ansource = ((JSONObject)o).getString("url");
            if(ansource.equals(urlSource)){
                break;
            }
        }
        return annotations;

    }
}
