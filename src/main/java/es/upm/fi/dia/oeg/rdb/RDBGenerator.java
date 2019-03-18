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
    private RDB rdb;


    public RDBGenerator(Dataset d){
       rmlc = d.getRmlcMappingY();
       csvw = d.getCsvw();
       csvs = d.getCsvFiles();
    }

    public void generateSchemaRDB(){
        normalize();
        String schema=createTables();
        rdb = new RDB("db",schema);

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
                if(((JSONObject) c).has("null")){
                    for(CSV csv: csvs) {
                        if(csv.getUrl().equals(csvURL)){
                            CSVUtils.putNull(csv.getRows(),(JSONObject) c);
                        }
                        else if(csv.getParentUrl()!=null && csv.getParentUrl().equals(csvURL)){
                            CSVUtils.putNull(csv.getRows(),(JSONObject) c);
                        }
                    }
                }
                if(((JSONObject) c).has("datatype")){
                    JSONObject datatype = ((JSONObject) c).getJSONObject("datatype");
                    String column = ((JSONObject) c).getString("titles");
                    if(datatype.has("format") && datatype.getString("base").equals("date")){
                        for(CSV csv : csvs){
                            if(csv.getUrl().equals(csvURL)){
                                CSVUtils.changeFormat(column,csv.getRows(),(JSONObject)c);
                            }
                            else if(csv.getParentUrl()!=null && csv.getParentUrl().equals(csvURL)){
                                CSVUtils.changeFormat(column,csv.getRows(),(JSONObject)c);
                            }
                        }
                    }
                }
                if(((JSONObject)c).has("default")){
                    for(CSV csv: csvs){
                        if(csv.getUrl().equals(csvURL)){
                            CSVUtils.putDefault(csv.getRows(),(JSONObject) c);
                        }
                        else if(csv.getParentUrl()!=null && csv.getParentUrl().equals(csvURL)){
                            CSVUtils.putDefault(csv.getRows(),(JSONObject) c);
                        }
                    }
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

    public void generateRDB(){
        RDBConexion rdbConexion = new RDBConexion();
        rdbConexion.createDatabase(rdb.getName(),rdb.getContent());
        HashMap<String,HashMap<String,String>> functions = new HashMap<>();
        HashMap<String,HashMap<String,String>> joinFunctions = new HashMap<>();
        rmlc.getTriples().forEach(triplesMap -> {
            for(CSV csv : csvs){
                if(csv.getUrl().equals(((Source)triplesMap.getLogicalSource()).getSourceName())){
                    String sourceUrl = ((Source) triplesMap.getLogicalSource()).getSourceName();
                    String tableName = sourceUrl.split("/")[sourceUrl.split("/").length-1].replace(".csv","").toUpperCase();
                    if(csv.getRows()==null){
                        String url = csv.getParentUrl();
                        for(CSV aux: csvs){
                            if(aux.getUrl().equals(url)){
                                rdbConexion.loadCSVinTable(triplesMap,aux.getRows(),tableName,rdb.getName());
                                break;
                            }
                        }

                    }
                    else {
                        rdbConexion.loadCSVinTable(triplesMap, csv.getRows(), tableName, rdb.getName());
                    }
                    break;
                }
            }
        });
        rdbConexion.addForeignKeys(rdb.getName());
        rmlc.getTriples().forEach(triplesMap -> {
            RDBUtils rdbutils = new RDBUtils();
            String sourceUrl = ((Source)triplesMap.getLogicalSource()).getSourceName();
            String tableName =sourceUrl.split("/")[sourceUrl.split("/").length-1].replace(".csv","").toUpperCase();
            functions.put(tableName,rdbutils.getColumnsFromFunctions(triplesMap.getPredicateObjectMaps()));
            joinFunctions.putAll(rdbutils.getJoinFunctions(triplesMap.getPredicateObjectMaps(),tableName));
        });
        rdbConexion.updateDataWithFunctions(functions,rdb.getName(),false);
        rdbConexion.updateDataWithFunctions(joinFunctions,rdb.getName(),true);

    }



}
