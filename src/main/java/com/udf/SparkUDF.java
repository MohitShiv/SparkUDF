package com.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SparkUDF implements UDF1<String, String>, Serializable {
    public static final String INVALID = "INVALID";
    private static final long serialVersionUID = 8044824939613747519L;
    private static final Logger LOG = LoggerFactory.getLogger(SparkUDF.class);


    @Override
    public String call(String arg) throws Exception {
        if (validateString(arg))
            return arg;
        return INVALID;
    }


    public boolean validateString(String arg) {

        if (arg == null | arg.length() != 10  //1.Head Num must be 10 character in length.
                | isStringContainSpecialCharacter(arg) // 2.Must not have symbol
        ) {
            return false;
        }
        return true;
    }

    public boolean isStringContainSpecialCharacter(String args) {
        Pattern pat = Pattern.compile("[^a-z0-9 ]", Pattern.CASE_INSENSITIVE);
        Matcher mat = pat.matcher(args);
        return mat.find();
    }

    public static boolean validateString1(String arg) {
        if (arg == null | arg.length() != 11)
            return false;
        else
            return true;
    }
}
