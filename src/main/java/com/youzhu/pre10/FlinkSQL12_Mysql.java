package com.youzhu.pre10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL12_Mysql {

    public static void main(String[] args) throws Exception {
        /*
        flinksql一定是读取结构化数据
        非结构化数据 流数据必须转换为动态表   才能实现flinksql实现需求
         */

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //注册SourceTable
        tableEnv.executeSql("create table source_sensor (id string,ts bigint,vc int) with ("
                + "'connector' = 'kafka',"
                + "'connector.topic' = 'topic_source',"
                + "'connector.properties.bootstrap.servers' = 'pre1:9092,pre1:9093,pre1:9094',"
                + "'connector.properties.group.id' = 'youzhu',"
                + "'scan.startup-mode' = 'latest',"
                + "'format' = 'csv'"
                + ")");

        //注册SinkTable:MySql  不会自动在Mysql创建表  需要手动在mysql中建表
        tableEnv.executeSql("create table sink_sensor (id string,ts bigint,vc int) with ("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://pre1:3306/test',"
                + "'table-name' = 'sink_table',"
                + "'username' = 'root',"
                + "'password' = 'aaaaaa'"
                + ")");

        tableEnv.sqlQuery("insert into sink_sensor select * from source_sensor ");


    }

}
