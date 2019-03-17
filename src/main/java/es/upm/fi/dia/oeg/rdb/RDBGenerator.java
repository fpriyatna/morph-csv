package es.upm.fi.dia.oeg.rdb;


import es.upm.fi.dia.oeg.model.*;

import es.upm.fi.dia.oeg.rmlc.api.model.ObjectMap;
import es.upm.fi.dia.oeg.rmlc.api.model.TriplesMap;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RDBGenerator {

    private RMLCMapping rmlc;
    private CSVW csvw;
    private ArrayList<CSV> csvs;


    public RDBGenerator(Dataset d){
        rmlc = d.getRmlcMappingY();
        csvw = d.getCsvw();
        csvs = d.getCsvFiles();
    }

    public RDB generateRDB(){

        basicSchemaGeneration();
        restriccionsGeneration();
        constraintsGeneration();

        return null;
    }

    private void basicSchemaGeneration(){

        JSONArray tables = (JSONArray) csvw.getContent().get("tables");

        for(Object o : tables){
            String csvURL = ((JSONObject) o).getString("url");
            if((((JSONObject) o).getJSONObject("tableSchema")).has("rowTitles")){
                JSONArray rows = (((JSONObject) o).getJSONObject("tableSchema")).getJSONArray("rowTitles");
                String[] csvHeader=rows.join(",").replaceAll("\"","").split(",");
                for(CSV csv : csvs){
                    if(csvURL.equals(csv.getUrl())){
                        csv.getRows().add(0,csvHeader);
                    }
                }
            }
            JSONArray columns = ((JSONObject) o).getJSONObject("tableSchema").getJSONArray("columns");
            List<String[]> newCSV = new ArrayList<>();
            for(Object c : columns){
                if(((JSONObject) c).has("separator")){
                    //create a new csv
                    String separator = ((JSONObject) c).getString("separator");
                    String column = ((JSONObject) c).getString("titles");
                    for(CSV csv: csvs){
                        if(csv.getUrl().equals(csvURL)){
                            MappingUtils u = new MappingUtils();
                            //create new csv
                            newCSV = CSVUtils.generateCSVfromSeparator(separator,column,csv.getRows(),u.getPKIndex(rmlc.getTriples(),csvURL,csv.getRows().get(0)));
                            //remove the column
                            csv.setRows(CSVUtils.removeSeparetedColumn(column,csv.getRows()));
                            //edit rmlcmappingnada nad
                            String newRMLC = u.generateTriplesMapfromSeparator(newCSV.get(0),rmlc,csvURL);
                            rmlc.setContent(newRMLC);
                            rmlc.setTriples(rmlc.getContent());

                        }
                    }
                    csvs.add(new CSV(column,newCSV));
                }
            }
        }




    }

    private void restriccionsGeneration(){

    }

    private void constraintsGeneration(){

    }



}
