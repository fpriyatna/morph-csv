package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.model.CSV;
import es.upm.fi.dia.oeg.rmlc.api.model.ObjectMap;
import org.apache.jena.atlas.json.JSON;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;

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

    public static JSONArray getAnnotationsFromSource (JSONArray tables, CSV csv){
        JSONArray annotations = null;
        for(Object o : tables){
            JSONArray aux = ((JSONObject) o).getJSONObject("tableSchema").getJSONArray("columns");
            String ansource = ((JSONObject)o).getString("url");
            if(ansource.equals(csv.getUrl())){
                annotations = aux;
                break;
            }
            else if(csv.getParentUrl()!=null && ansource.equals(csv.getParentUrl())){
                annotations = aux;
                break;
            }
        }


        return annotations;

    }

    public static JSONObject annotationForID(){
        JSONObject id = new JSONObject();
        HashMap<String,String> datatype = new HashMap<>();
        datatype.put("base","integer");
        id.put("titles","id");
        id.put("datatype",datatype);


        return id;
    }

    public static JSONObject annotationForJOIN(String column){
        JSONObject id = new JSONObject();
        HashMap<String,String> datatype = new HashMap<>();
        datatype.put("base","integer");
        id.put("titles",column+"_J");
        id.put("datatype",datatype);
        return id;
    }
}
