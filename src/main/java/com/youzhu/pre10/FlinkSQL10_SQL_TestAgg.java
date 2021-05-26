package com.youzhu.pre10;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL10_SQL_TestAgg {

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
        //将流进行表注册
        tableEnv.createTemporaryView("sensor",waterSensorDS);

        //使用SQL查询注册表的
        Table result = tableEnv.sqlQuery("select id,count(ts) ct,sum(vc) vc_sum from sensor group by id");

        //将表对象转换为流进行打印输出
        tableEnv.toRetractStream(result, Row.class).print();

        //执行任务
        env.execute();
    }

}
