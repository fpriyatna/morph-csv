package es.upm.fi.dia.oeg.utils;


import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileReader;

public class Utils {



    public static JSONArray readConfiguration (String config){
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject = new JSONObject(IOUtils.toString(new FileReader(config)));
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        return jsonObject.getJSONArray("sources");
    }



}
