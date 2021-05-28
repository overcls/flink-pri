package com.youzhu.pre12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkSQL08_HiveCatalog {

    public static void main(String[] args) {

        /*
        flink 对接hive  使用流处理   使用catalog  表数据就可以跨会话使用
         */

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "default", "input");

        //3.注册catalog
        tableEnv.registerCatalog("myhive",hiveCatalog);

        //4.使用catalog
        tableEnv.useCatalog("myhive");

        //5.执行查询 , 查询hive中已经存在的表数据
        tableEnv.executeSql("select * from table_name").print();


    }

}
