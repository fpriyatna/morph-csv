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
            r2rml = getPrefix(utils.getMappingContent(utils.getMappingPath(file_name)));
            triplesMaps.forEach(triplesMap -> {
                r2rml += "<"+triplesMap.getNode().ntriplesString().split("#")[1]+"\n";
                r2rml += createLogicalTable(triplesMap.getLogicalSource());
                r2rml += createSubjectMap(triplesMap.getSubjectMap());
                r2rml += createPredicatesObjectMaps(triplesMap.getPredicateObjectMaps());
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

        return subject+"\t];";
    }

    private String createPredicatesObjectMaps(List<PredicateObjectMap> predicateObjectMaps){
        String predicates="";
        for(PredicateObjectMap predicateObjectMap : predicateObjectMaps){
            predicates +="\trr:predicateObjectMap [ \n";
            for(PredicateMap predicateMap : predicateObjectMap.getPredicateMaps()){
                predicates +="\t\t rr:predicateMap [ rr:constant <"+predicateMap.getConstant().ntriplesString()+">];";
            }
            for (ObjectMap objectMap : predicateObjectMap.getObjectMaps()){
                predicates += createObjectMap(objectMap);
            }
            for(RefObjectMap refObjectMap : predicateObjectMap.getRefObjectMaps()){
                predicates += createRefObjectMap(refObjectMap);
            }

        }

        return predicates+"\t];";
    }

    private String createRefObjectMap(RefObjectMap refObjectMaps){
        String reference ="";

        return reference;
    }

    private String createObjectMap (ObjectMap objectMap){
        String reference ="";

        return reference;

    }

}
