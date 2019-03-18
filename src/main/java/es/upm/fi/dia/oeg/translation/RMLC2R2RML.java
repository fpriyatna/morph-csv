package es.upm.fi.dia.oeg.translation;


import es.upm.fi.dia.oeg.model.RMLCMapping;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

public class RMLC2R2RML {

    private String r2rml;

    public String getR2RML(){
        return r2rml;
    }

    public void generateR2RML(RMLCMapping rmlc){
        r2rml ="";
        Collection<TriplesMap> triplesMaps = rmlc.getTriples();
        r2rml = getPrefix(rmlc.getContent())+"\n\n";
        triplesMaps.forEach(triplesMap -> {
            if(triplesMap.getNode().ntriplesString().matches(".*#.*"))
                r2rml += "<"+triplesMap.getNode().ntriplesString().split("#")[1]+"\n";
            else
                r2rml += "<"+triplesMap.getNode().ntriplesString().split("/")[triplesMap.getNode().ntriplesString().split("/").length-1]+"\n";
            r2rml += createLogicalTable(triplesMap.getLogicalSource());
            r2rml += createSubjectMap(triplesMap.getSubjectMap());
            r2rml += createPredicatesObjectMaps(triplesMap.getPredicateObjectMaps())+".\n\n";
        });
        try {
            BufferedWriter writer = new BufferedWriter
                    (new OutputStreamWriter(new FileOutputStream("mapping.r2rml.ttl"), StandardCharsets.UTF_8));
            writer.write(r2rml);
            writer.close();
        }catch (Exception e){

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
                +((Source) logicalSource).getSourceName().split("/")[((Source) logicalSource).getSourceName().split("/").length-1].replace(".csv","").toUpperCase()
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
        String reference = "";
        if(objectMap.getFunction()!=null && !objectMap.getFunction().isEmpty()){
            for(PredicateMap p: predicateMaps) {
                String column_name=p.getConstant().ntriplesString();
                if(p.getConstant().ntriplesString().matches(".*#.*")){
                    column_name =column_name.split("#")[1].replace(">", "");
                }
                else{
                    column_name=column_name.split("/")[column_name.split("/").length-1].replace(">", "");
                }
                reference += "\t\trr:objectMap[\n\t\t\trr:column \"" +column_name+ "\";\n\t\t];\n";
            }
        }
        if(objectMap.getDatatype()!=null && !objectMap.getDatatype().getIRIString().isEmpty()){
            reference += "\t\trr:objectMap[\n\t\t\trr:datatype <"+objectMap.getDatatype().getIRIString()+">;\n\t\t];\n";
        }
        if(objectMap.getColumn()!=null && !objectMap.getColumn().isEmpty()){
            reference += "\t\trr:objectMap[\n\t\t\trr:column \""+objectMap.getColumn()+"\";\n\t\t];\n";
        }
        if(objectMap.getTemplate()!=null && !objectMap.getTemplateString().isEmpty()){
            reference +="\t\trr:objectMap[\n\t\t\trr:template \""+objectMap.getTemplateString()+"\";\n\t\t];\n";
        }
        if(objectMap.getConstant()!=null && !objectMap.getConstant().ntriplesString().isEmpty()){
            reference +="\t\trr:objectMap[\n\t\t\trr:constant "+objectMap.getConstant().ntriplesString()+";\n\t\t];\n";
        }
        return reference;

    }

    private String createRefObjectMap(RefObjectMap refObjectMap, PredicateMap predicateMap){
        String reference = "\t\trr:objectMap [\n";
        if(refObjectMap.getParentMap().getNode().ntriplesString().matches(".*#.*"))
            reference += "\t\t\trr:parentTriplesMap <"+refObjectMap.getParentMap().getNode().ntriplesString().split("#")[1]+";\n";
        else{
            String r = refObjectMap.getParentMap().getNode().ntriplesString().split("/")[refObjectMap.getParentMap().getNode().ntriplesString().split("/").length-1];
            reference += "\t\t\trr:parentTriplesMap <"+r+";\n";
        }

        for(Join j : refObjectMap.getJoinConditions()) {
            reference += "\t\t\trr:joinCondition [\n";
            if(j.getChild().matches(".*\\(.*")){
                if(predicateMap.getConstant().ntriplesString().matches(".*#.*"))
                    reference += "\t\t\t\t rr:child \"" + predicateMap.getConstant().ntriplesString().split("#")[1].replace(">", "") + "\";\n";
                else {
                    String c = predicateMap.getConstant().ntriplesString().split("/")[predicateMap.getConstant().ntriplesString().split("/").length - 1];
                    reference +="\t\t\t\t rr:child \"" + c.replace(">", "") + "\";\n";
                }
            }
            else{
                    reference+="\t\t\t\t rr:child \""+j.getChild().replace("{","").replace("}","")+"\";\n";
            }
            if(j.getParent().matches(".*\\(.*")) {
                if (predicateMap.getConstant().ntriplesString().matches(".*#.*")) {
                    reference += "\t\t\t\t rr:parent \"" + predicateMap.getConstant().ntriplesString().split("#")[1].replace(">", "") + "\";\n";
                } else {
                    String c = predicateMap.getConstant().ntriplesString().split("/")[predicateMap.getConstant().ntriplesString().split("/").length - 1];
                    reference += "\t\t\t\t rr:parent \"" + c.replace(">", "") + "\";\n";
                }
            }
            else{
                reference+="\t\t\t\t rr:parent \""+j.getParent().replace("{","").replace("}","")+"\";\n";
            }


            reference += "\t\t\t];\n";
        }

        return reference+"\t\t];\n";
    }



}
