package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.model.RMLCMapping;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
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

    public HashMap<String,ArrayList<String>> getForeignKeys (List<PredicateObjectMap> pom, String[] headers){
        HashMap<String,ArrayList<String>> foreignKeys = new HashMap<>();

        for (PredicateObjectMap po : pom){
            for (RefObjectMap refObjectMap : po.getRefObjectMaps()){
                ArrayList<String> fkreference = new ArrayList<>();boolean parentflag=false,childflag=false;
                String parent = refObjectMap.getJoinCondition(0).getParent();
                String child = refObjectMap.getJoinCondition(0).getChild();
                for(String header : headers){
                    if(parent.matches(header)){
                        parentflag = true;
                    }
                    if(child.matches(header)){
                        childflag = true;
                    }
                }

                if(!parentflag){
                    parent = po.getPredicateMap(0).getConstant().ntriplesString();
                }
                if(!childflag){
                    child = po.getPredicateMap(0).getConstant().ntriplesString();
                }

                fkreference.add(child);
                fkreference.add(parent);
                String tableName = ((Source)refObjectMap.getParentMap().getLogicalSource()).getSourceName();
                tableName = tableName.split("/")[tableName.split("/").length-1].replace(".csv","").toUpperCase();
                foreignKeys.put(tableName,fkreference);
            }
        }


        return foreignKeys;
    }

    public String generateTriplesMapfromSeparator(String[] headers,RMLCMapping rmlc, String sourceUrl){
        String separetedColumn = headers[headers.length-1].trim();
        StringBuilder tripleMap = new StringBuilder();

        tripleMap.append("<"+separetedColumn+">\n");
        //logicalSource
        tripleMap.append("\trml:logicalSource [\n\t\trml:source \""+separetedColumn+".csv\";\n\t\trml:referenceFormulation ql:CSV\n\t];\n");
        //subjectMap
        tripleMap.append("\trr:subjectMap [\n\t\t rr:template \"http://ex.com/"+separetedColumn+"/");
        for(String s : headers){
            tripleMap.append("{"+s.trim()+"}");
        }
        tripleMap.append("\";\n\t];\n");

        tripleMap.append("\trr:predicateObjectMap[\n");
        tripleMap.append("\t\trr:predicate ex:"+separetedColumn+";\n");
        tripleMap.append("\t\trr:objectMap [\n");
        tripleMap.append("\t\t\trml:reference \""+separetedColumn+"\";\n");
        tripleMap.append("\t\t];\n\n\t];\n.");
        StringBuilder changeRmlcContet = fromObjectToRefObjectMap(rmlc.getContent(),headers,sourceUrl);
        changeRmlcContet.append(tripleMap.toString());
        return changeRmlcContet.toString();

    }


    private StringBuilder fromObjectToRefObjectMap(String content, String[] headers, String sourceUrl){
        StringBuilder join = new StringBuilder();
        StringBuilder finalcontent= new StringBuilder();
        String column = headers[headers.length-1].trim();
        for(String s : headers){
            if(!s.trim().equals(column.trim()))
                join.append("{"+s.trim()+"}");
        }
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
                    splitedContent.add(i+2,"\t\t\t\trmlc:child \""+join.toString()+"\";");
                    splitedContent.add(i+3,"\t\t\t\trmlc:parent \""+join.toString()+"\";");
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
