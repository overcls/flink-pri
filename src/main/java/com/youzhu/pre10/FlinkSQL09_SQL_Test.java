package com.youzhu.pre10;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL09_SQL_Test {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });
        //将流转换为动态表   使用元组时要定义属性  使用$()定义字段名  JavaBean不需要
        Table table = tableEnv.fromDataStream(waterSensorDS);

        //使用SQL查询未注册表的
        Table result = tableEnv.sqlQuery("select id,ts,vc from " + table + " where id = 'ws_001'");

        //将表对象转换为流进行打印输出
        tableEnv.toAppendStream(result, Row.class).print();

        //执行任务
        env.execute();
    }

}
