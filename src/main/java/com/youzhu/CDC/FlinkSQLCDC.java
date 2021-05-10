package com.youzhu.CDC;

import com.mysql.cj.result.Row;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {

        //创建执行环境

        //flink sql 只能读取单表 csv直接切割就可用数据
        //datastream 适合全库扫描  数据格式 不好用
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取Mysql数据
        tableEnv.executeSql("CREATE TABLE mysql_binlog(\n" +
                "id INT NOT NULL , \n" +
                "name STRING, \n" +
                "description STRING, \n" +
                "weight DECIMAL(10,3) \n" +
                ")WITH (\n" +
                " 'connector ' = 'mysql-cdc', \n" +
                "'hostname' = 'ip'" +
                "'port' = '3306',\n" +
                "'username' = 'flinkuser',\n" +
                "'password' = 'flinkpw',\n " +
                "'database-name' = 'inventory',\n" +
                "'table-name' = 'products' \n" +
                ");"
        );

        //打印

        Table table = tableEnv.sqlQuery("select * from table_name");
        //有CRUD操作 使用撤回流
        tableEnv.toRetractStream(table, Row.class).print();

        //开启任务

        env.execute();
    }
}
