package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.model.RMLCMapping;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.apache.commons.rdf.api.RDFTerm;

import java.util.*;

public class MappingUtils {

    private List<String> subjectMapColumns;

    public List<Integer> getPKIndex(Collection<TriplesMap> triplesMaps, String sourceUrl, String[] csvHeaders){
        List<Integer> pkIndex = new ArrayList<>();
        triplesMaps.forEach(tripleMap -> {
            if(((Source)tripleMap.getLogicalSource()).getSourceName().equals(sourceUrl)){
                subjectMapColumns = tripleMap.getSubjectMap().getTemplate().getColumnNames();
            }
        });

        for(String column: subjectMapColumns){
            for(int i=0; i<csvHeaders.length; i++){
                if(csvHeaders[i].trim().equals(column.trim())){
                    pkIndex.add(i);
                }
            }
        }
        return pkIndex;
    }

    /*public List<String> getPkArray(Collection<TriplesMap> triplesMaps, String sourceUrl){

        triplesMaps.forEach(tripleMap -> {
            if(((Source)tripleMap.getLogicalSource()).getSourceName().equals(sourceUrl)){
                subjectMapColumns = tripleMap.getSubjectMap().getTemplate().getColumnNames();
            }
        });

        return subjectMapColumns;
    }*/

    public ArrayList<String> getPrimaryKeys(SubjectMap s){
        ArrayList<String> primaryKeys = new ArrayList<>();

        if(s.getColumn()!=null){
            primaryKeys.add(s.getColumn());
        }
        else if(s.getTemplateString()!=null){
            for(String t : s.getTemplate().getColumnNames()){
                primaryKeys.add(t);
            }
        }
        return primaryKeys;

    }

    public HashMap<String,ArrayList<String>> getForeignKeys (TriplesMap tp){
        HashMap<String,ArrayList<String>> foreignKeys = new HashMap<>();
        //todo foreign keys

        for (PredicateObjectMap po : tp.getPredicateObjectMaps()) {
            for (RefObjectMap refObjectMap : po.getRefObjectMaps()) {
                ArrayList<String> fkreference = new ArrayList<>();
                ArrayList<String> parent = getColumnsFromTemplate(refObjectMap.getJoinCondition(0).getParent());
                ArrayList<String> child = getColumnsFromTemplate(refObjectMap.getJoinCondition(0).getChild());
                boolean isPK = isPKinParentTripleMap(refObjectMap.getParentMap().getSubjectMap(),parent);
                if (!parent.isEmpty() && !child.isEmpty() && isPK) {
                    String pcolumns = "", ccolumns = "";
                    for (String p : parent) {
                        pcolumns += p + ",";
                    }
                    for (String c : child) {
                        ccolumns += c + ",";
                    }
                    fkreference.add(ccolumns.substring(0, ccolumns.length() - 1));
                    fkreference.add(pcolumns.substring(0, pcolumns.length() - 1));
                    String tableName = ((Source) refObjectMap.getParentMap().getLogicalSource()).getSourceName();
                    tableName = tableName.split("/")[tableName.split("/").length - 1].replace(".csv", "").toUpperCase();
                    foreignKeys.put(tableName, fkreference);
                }

            }
        }



        return foreignKeys;
    }


    private ArrayList<String> getColumnsFromTemplate(String template){
        ArrayList<String> fk = new ArrayList<>();
        if(!template.contains("(")) {
            String[] aux = template.split("\\{");
            for (int i = 1; i < aux.length; i++) {
                fk.add(aux[i].split("}")[0]);
            }
        }
        return fk;
    }

    private boolean isPKinParentTripleMap(SubjectMap s, ArrayList<String> parent){
        boolean checker = false;
        Integer cont = 0;
        List<String> pks = s.getTemplate().getColumnNames();
        if(pks.size()==parent.size()){
            for(String pk : pks){
                if(parent.contains(pk)){
                    cont++;
                }
            }
        }
        if(cont == pks.size()){
            checker = true;
        }

        return checker;
    }

    public String generateTriplesMapfromSeparator(String[] headers,RMLCMapping rmlc, String sourceUrl){
        String id = headers[0];
        String values = headers[1];
        StringBuilder tripleMap = new StringBuilder();

        tripleMap.append("<"+values+">\n");
        //logicalSource
        tripleMap.append("\trml:logicalSource [\n\t\trml:source \""+values+".csv\";\n\t\trml:referenceFormulation ql:CSV\n\t];\n");
        //subjectMap
        tripleMap.append("\trr:subjectMap [\n\t\t rr:template \"http://ex.com/"+values+"/{"+id+"}-{"+values+"}");

        tripleMap.append("\";\n\t];\n");

        tripleMap.append("\trr:predicateObjectMap[\n");
        tripleMap.append("\t\trr:predicate ex:"+values+";\n");
        tripleMap.append("\t\trr:objectMap [\n");
        tripleMap.append("\t\t\trml:reference \""+values+"\";\n");
        tripleMap.append("\t\t];\n\n\t];\n.");
        StringBuilder changeRmlcContet = fromObjectToRefObjectMap(rmlc.getContent(),headers,sourceUrl);
        changeRmlcContet.append(tripleMap.toString());
        return changeRmlcContet.toString();

    }


    private StringBuilder fromObjectToRefObjectMap(String content, String[] headers, String sourceUrl){
        StringBuilder finalcontent= new StringBuilder();
        String parentJoin = headers[0];
        String column = headers[1];

        ArrayList<String> splitedContent = new ArrayList<>(Arrays.asList(content.split("\n")));
        boolean flag = false;
        for(int i=0; i<splitedContent.size();i++){
            if(splitedContent.get(i).matches(".*"+sourceUrl+".*")){
                flag= true;
            }
            if(flag){
                if(splitedContent.get(i).matches(".*"+column+".*")){
                    splitedContent.set(i,"\t\t\trr:parentTriplesMap <"+column+">;");
                    splitedContent.add(i+1,"\t\t\trmlc:joinCondition [");
                    splitedContent.add(i+2,"\t\t\t\trmlc:child \"{"+column+"_J}\";");
                    splitedContent.add(i+3,"\t\t\t\trmlc:parent \"{"+parentJoin+"}\";");
                    splitedContent.add(i+4,"\t\t\t];");
                    break;
                }
            }
        }

        for(String s: splitedContent){
            finalcontent.append(s+"\n");
        }
        return finalcontent;

    }
}
