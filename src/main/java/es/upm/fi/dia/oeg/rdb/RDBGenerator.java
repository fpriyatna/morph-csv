package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.model.*;

import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        normalize();
        String schema=createTables();
        RDB rdb = new RDB("someName",schema);
        return rdb;
    }

    private void normalize(){

        JSONArray tables = (JSONArray) csvw.getContent().get("tables");
        //FN1
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
            JSONArray newAnnotations = new JSONArray();
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
                            newCSV = CSVUtils.generateCSVfromSeparator(separator,column,csv.getRows());
                            //remove the column
                            csv.setRows(CSVUtils.removeSeparetedColumn(column,csv.getRows()));
                            //edit rmlcmappingnada nad
                            String newRMLC = u.generateTriplesMapfromSeparator(newCSV.get(0),rmlc,csvURL);
                            JSONObject idannotations = CSVWUtils.annotationForID();
                            newAnnotations.put(idannotations);
                            idannotations = CSVWUtils.annotationForJOIN(column);
                            newAnnotations.put(idannotations);
                            rmlc.setContent(newRMLC);
                            rmlc.setTriples(rmlc.getContent());

                        }
                    }
                    csvs.add(new CSV(column,column+".csv",csvURL,newCSV));
                }
            }
            for(Object c: newAnnotations){
                columns.put(c);
            }
        }

        //FN2
        HashMap<String, Boolean> sources = new HashMap<>();
        rmlc.getTriples().forEach(triplesMap -> {
            if(sources.get(((Source)triplesMap.getLogicalSource()).getSourceName())==null)
               sources.put(((Source)triplesMap.getLogicalSource()).getSourceName(),false);
            else
               sources.put(((Source)triplesMap.getLogicalSource()).getSourceName(),true);
        });

        for(Map.Entry<String, Boolean> entry : sources.entrySet()){
            if(entry.getValue()){
                ArrayList<TriplesMap> triplesMaps = new ArrayList<>();
                rmlc.getTriples().forEach(tp -> {
                    if(((Source)tp.getLogicalSource()).getSourceName().equals(entry.getKey())){
                        triplesMaps.add(tp);
                    }
                });

                for(TriplesMap t : triplesMaps){
                    String newSourceName=""; TriplesMap aux=null;
                    for(PredicateObjectMap pom : t.getPredicateObjectMaps()){
                        for(RefObjectMap ob : pom.getRefObjectMaps()){
                            String columnWithoutColumns = ob.getParentMap().getSubjectMap().getTemplate().getTemplateStringWithoutColumnNames().
                                    replace("{","").replace("}-","").replace("}","");
                            String pkExpresion= ob.getParentMap().getSubjectMap().getTemplateString().replace(columnWithoutColumns,"");
                            ob.getJoinCondition(0).setChild(pkExpresion);
                            ob.getJoinCondition(0).setParent(pkExpresion);
                            newSourceName = ob.getParentMap().getNode().ntriplesString().replace(">","").replace("<","");
                            aux = ob.getParentMap();

                        }
                    }
                    if(newSourceName!="" && aux!=null){
                        String oldSource = ((Source)aux.getLogicalSource()).getSourceName();
                        ((Source)aux.getLogicalSource()).setSourceName(newSourceName+".csv");
                        csvs.add(new CSV(newSourceName,newSourceName+".csv",oldSource,null));
                    }
                }
            }
        }


    }

    private String createTables(){
        RDBUtils rdbUtils = new RDBUtils();
        return rdbUtils.createSQLSchema(rmlc.getTriples(),csvs,csvw);
    }

    private void constraintsGeneration(){

    }



}
