package es.upm.fi.dia.oeg.rdb;




public class DatePatterns {

    //date

    public static final String point = "([12]\\d{3}\\.(0[1-9]|1[0-2])\\.(0[1-9]|[12]\\d|3[01]))";
    public static final String backslash = "([12]\\d{3}/(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01]))";
    public static final String line = "([12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))";
    public static final String wihtout_space = "([12]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01]))";

    public static final String inv_point = "(0[1-9]|[12]\\d|3[01])\\.(0[1-9]|1[0-2])\\.([12]\\d{3})";
    public static final String inv_line = "(0[1-9]|[12]\\d|3[01])-(0[1-9]|1[0-2])-([12]\\d{3})";
    public static final String inv_backslash = "(0[1-9]|[12]\\d|3[01])/(0[1-9]|1[0-2])/([12]\\d{3})";
    public static final String inv_wihtout_space = "(0[1-9]|[12]\\d|3[01])(0[1-9]|1[0-2])([12]\\d{3})";


    //datetime


}
