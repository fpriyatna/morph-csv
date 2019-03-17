package es.upm.fi.dia.oeg.rdb;


import java.util.ArrayList;
import java.util.List;

public class CSVUtils {


    public static List<String[]> generateCSVfromSeparator(String separator, String column, List<String[]> csv, List<Integer> pk){

        String[] headers = csv.get(0);

        List<String[]> separatedCSV = new ArrayList<>();
        Integer index = getIndexColumnFromHeader(headers,column);
        for (String[] rows : csv){
            String[] data = rows[index].split(separator);
            for(String d : data){
                ArrayList<String> finalRow = new ArrayList<>();
                for(Integer pkindex : pk){
                    finalRow.add(rows[pkindex]);
                }
                finalRow.add(d);
                separatedCSV.add(finalRow.toArray(new String[finalRow.size()]));
            }
        }


        return separatedCSV;
    }

    public static List<String[]> removeSeparetedColumn(String column, List<String[]> csv){
        String[] headers = csv.get(0);
        List<String[]> cleanedCSV = new ArrayList<>();
        Integer index= getIndexColumnFromHeader(headers,column);

        for(int i=0; i<csv.size() ;i++){
            ArrayList<String> row = new ArrayList<>();
            for(int j=0; j< csv.get(i).length ; j++) {
                if (index != j) {
                    row.add(csv.get(i)[j]);
                }
            }
            cleanedCSV.add(row.toArray(new String[row.size()]));
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
