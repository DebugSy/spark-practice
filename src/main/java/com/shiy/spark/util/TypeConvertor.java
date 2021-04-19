package com.shiy.spark.util;

/**
 * @Author: haijun
 * @Date: 2020/6/16 19:54
 */
public class TypeConvertor {
    /**
     * hbase decimal type convertor
     * @param fieldType
     * @return
     */
    public static String convertDecimal(String fieldType){
        String typeInJson = fieldType;
        int begin = typeInJson.indexOf("(");
        int end = typeInJson.indexOf(")");
        int sep = typeInJson.indexOf(",");
        if(begin>0 && end>0 && sep > 0){
            int precision = Integer.parseInt(typeInJson.substring(begin + 1, sep).trim());
            int scale = Integer.parseInt(typeInJson.substring(sep + 1, end).trim());

            if (scale == 0) {
                if (precision == 1) {
                    typeInJson = "int";
                } else if (precision < 4) {
                    typeInJson = "int";
                } else if (precision < 6) {
                    typeInJson = "int";
                } else if (precision < 11) {
                    typeInJson = "int";
                } else if (precision < 39) {
                    typeInJson = "bigint";
                } else{
                    typeInJson = "bigint";
                }
            } else {
                typeInJson = "double";
            }
        }else {
            typeInJson = "double";
        }
        return typeInJson;
    }
}
