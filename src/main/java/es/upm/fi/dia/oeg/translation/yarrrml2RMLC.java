package es.upm.fi.dia.oeg.translation;

import es.upm.dia.oeg.Yarrrml2rmlc;
import es.upm.fi.dia.oeg.model.RMLCMapping;
import es.upm.fi.dia.oeg.model.YarrrmlMapping;

public class yarrrml2RMLC {

    public static RMLCMapping translateYarrrml2RMLC(YarrrmlMapping ymapping){
        String rmlcContent = Yarrrml2rmlc.translateYarrrml2RMLC(ymapping.getContent());
        RMLCMapping rmlcMappingY = new RMLCMapping(rmlcContent);
        return rmlcMappingY;
    }

}
