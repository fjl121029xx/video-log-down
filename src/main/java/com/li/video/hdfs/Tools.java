package com.li.video.hdfs;

import org.apache.hadoop.conf.Configuration;

import java.util.Date;

public class Tools {

    public static final org.apache.hadoop.conf.Configuration Configuration =
            new Configuration();

    static {
        Configuration.set("fs.defaultFS", "hdfs://ns1/");
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    public static void main(String[] args) {

        Date d = new Date(1536547094000L);

        System.out.println(d);
    }
}
