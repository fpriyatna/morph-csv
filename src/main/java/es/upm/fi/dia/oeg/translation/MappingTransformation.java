package es.upm.fi.dia.oeg.translation;

import es.upm.fi.dia.oeg.Utils;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MappingTransformation {

    private String r2rml="";

    public void generateR2RML(Utils utils){
        HashMap<String, Collection<TriplesMap>> triples=utils.getMappings();
        for(Map.Entry<String, Collection<TriplesMap>> entry : triples.entrySet()){
            String file_name = entry.getKey();
            Collection<TriplesMap> triplesMaps = entry.getValue();
            r2rml = getPrefix(utils.getMappingContent(utils.getMappingPath(file_name)))+"\n\n";
            triplesMaps.forEach(triplesMap -> {
                r2rml += "<"+triplesMap.getNode().ntriplesString().split("#")[1]+"\n";
                r2rml += createLogicalTable(triplesMap.getLogicalSource());
                r2rml += createSubjectMap(triplesMap.getSubjectMap());
                r2rml += createPredicatesObjectMaps(triplesMap.getPredicateObjectMaps())+".\n\n";
            });
            try {
                BufferedWriter writer = new BufferedWriter
                        (new OutputStreamWriter(new FileOutputStream("datasets/" + file_name + "/mapping.r2rml.ttl"), StandardCharsets.UTF_8));
                writer.write(r2rml);
                writer.close();
            }catch (Exception e){

            }
        }
    }

    private String getPrefix(String mappingContent){
        String prefixes = "";
        String[] lines = mappingContent.split("\\.\n");
        int i=0;
        while (lines[i].startsWith("@")){
                prefixes += lines[i]+".\n";
                i++;
        }
        return prefixes;
    }

    private String createLogicalTable(LogicalSource logicalSource){
       return "\trr:logicalTable [ \n\t\trr:tableName \"\\\""
                +((SQLBaseTableOrView) logicalSource).getTableName().toLowerCase().replace(".csv","")
                +"\\\"\"; \n\t];\n";
    }

    private String createSubjectMap(SubjectMap subjectMap){
        String subject="\trr:subjectMap [ \n\t\ta rr Subject;\n";

        if(!subjectMap.getTemplateString().isEmpty()){
            subject += "\t\trr:template \"" + subjectMap.getTemplateString() +"\";\n";
        }
        if(!subjectMap.getTermType().getIRIString().isEmpty()){
            subject += "\t\trr:termType <" +subjectMap.getTermType().getIRIString() +">;\n";
        }
        for(IRI iri : subjectMap.getClasses()){
            subject += "\t\trr:class <"+ iri.getIRIString() +">;\n";
        }

        return subject+"\t];\n";
    }

    private String createPredicatesObjectMaps(List<PredicateObjectMap> predicateObjectMaps){
        String predicates="";
        for(PredicateObjectMap predicateObjectMap : predicateObjectMaps){
            predicates +="\trr:predicateObjectMap [ \n";
            for(PredicateMap predicateMap : predicateObjectMap.getPredicateMaps()){
                predicates +="\t\trr:predicateMap [ rr:constant "+predicateMap.getConstant().ntriplesString()+" ];\n";
            }
            for (ObjectMap objectMap : predicateObjectMap.getObjectMaps()){
                predicates += createObjectMap(objectMap,predicateObjectMap.getPredicateMaps());
            }
            for(RefObjectMap refObjectMap : predicateObjectMap.getRefObjectMaps()){
                predicates += createRefObjectMap(refObjectMap,predicateObjectMap.getPredicateMaps().get(0));
            }
            predicates+="\t];\n";
        }

        return predicates;
    }

    private String createObjectMap (ObjectMap objectMap, List<PredicateMap> predicateMaps){
        String reference = "\t\trr:objectMap [\n";
        if(objectMap.getFunction()!=null && !objectMap.getFunction().isEmpty()){
            for(PredicateMap p: predicateMaps) {
                String column_name=p.getConstant().ntriplesString();
                if(p.getConstant().ntriplesString().matches(".*#.*")){
                    column_name =column_name.split("#")[1].replace(">", "");
                }
                else{
                    column_name=column_name.split("/")[column_name.split("/").length-1].replace(">", "");
                }
                reference += "\t\t\trr:column \"" +column_name+ "\";\n";
            }
        }
        if(objectMap.getDatatype()!=null && !objectMap.getDatatype().getIRIString().isEmpty()){
            reference += "\t\t\trr:datatype <"+objectMap.getDatatype().getIRIString()+">;\n";
        }
        if(objectMap.getColumn()!=null && !objectMap.getColumn().isEmpty()){
            reference += "\t\t\trr:column \""+objectMap.getColumn()+"\";\n";
        }
        if(objectMap.getTemplate()!=null && !objectMap.getTemplateString().isEmpty()){
            reference +="\t\t\trr:template \""+objectMap.getTemplateString()+"\";\n";
        }
        return reference+"\t\t];\n";

    }

    private String createRefObjectMap(RefObjectMap refObjectMap, PredicateMap predicateMap){
        String reference = "\t\trr:objectMap [\n";
        reference += "\t\t\trr:parentTriplesMap <"+refObjectMap.getParentMap().getNode().ntriplesString().split("#")[1]+";\n";
        for(Join j : refObjectMap.getJoinConditions()) {
            reference += "\t\t\trr:joinCondition [\n";
            reference += "\t\t\t\t rr:child \\\""+predicateMap.getConstant().ntriplesString().split("#")[1].replace(">", "")+"\\\";\n";
            reference += "\t\t\t\t rr:parent \"\\\""+predicateMap.getConstant().ntriplesString().split("#")[1].replace(">", "")+"\\\"\";\n";
            reference += "\t\t\t];\n";
        }

        return reference+"\t\t];\n";
    }



}
