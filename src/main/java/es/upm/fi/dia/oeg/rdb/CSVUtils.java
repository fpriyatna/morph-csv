package es.upm.fi.dia.oeg.rdb;


import java.util.ArrayList;
import java.util.List;

public class CSVUtils {


    public static List<String[]> generateCSVfromSeparator(String separator, String column, List<String[]> csv){

        String[] headers = csv.get(0);

        List<String[]> separatedCSV = new ArrayList<>();
        separatedCSV.add(new String[]{"id", column});
        Integer index = getIndexColumnFromHeader(headers,column);
        Integer id =0;
        for (String[] rows : csv){
            String[] data = rows[index].split(separator);
            for(String d : data){
                separatedCSV.add(new String[]{Integer.toString(id),d});
            }
            id++;
        }


        return separatedCSV;
    }

    public static List<String[]> removeSeparetedColumn(String column, List<String[]> csv){
        String[] headers = csv.get(0);
        List<String[]> cleanedCSV = new ArrayList<>();
        Integer index= getIndexColumnFromHeader(headers,column);
        Integer fkid=0;
        for(int i=0; i<csv.size() ;i++){
            String[] row = csv.get(i);
            if(i!=0) {
                row[index] = Integer.toString(fkid);
                fkid++;
            }
            else {
                row[index] = row[index]+"_J";
            }
            cleanedCSV.add(row);
        }
        return  cleanedCSV;
    }

    private static Integer getIndexColumnFromHeader(String[] headers, String column){
        Integer index =0;
        for(int i=0; i<headers.length;i++){
            if(headers[i].trim().equals(column.trim())){
                index = i;
            }

        }
        return index;
    }
}
