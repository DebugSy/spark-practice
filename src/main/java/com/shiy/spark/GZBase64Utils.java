package com.shiy.spark;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZBase64Utils {

    public static final String DEFAULT_ENCODING = "UTF-8";

    public static String uncompressString(String zippedBase64Str) throws IOException {
        String result;
        byte[] bytes = Base64.getDecoder().decode(zippedBase64Str);
        GZIPInputStream zi = null;
        try {
            zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
            result = IOUtils.toString(zi, DEFAULT_ENCODING);
        } finally {
            IOUtils.closeQuietly(zi);
        }
        return result;
    }

    public static String compressString(String srcTxt)
            throws IOException {
        ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
        GZIPOutputStream zos = new GZIPOutputStream(rstBao);
        zos.write(srcTxt.getBytes(DEFAULT_ENCODING));
        IOUtils.closeQuietly(zos);

        byte[] bytes = rstBao.toByteArray();
        return Base64.getEncoder().encodeToString(bytes);
    }

}
