package com.youzhu.pre3;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Transform_MaxBy {
/*
max  只会保证 聚酸结果为最大值  其与各个结果为第一次输入的值
maxby 会将一条值都进行更新
 */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> localhost = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //按照传感器id分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = localhost.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        //计算最高水位线   first 的boolean值控制keyby以外的其他字段 要不要更新数据
        SingleOutputStreamOperator<WaterSensor> vc = waterSensorStringKeyedStream.maxBy("vc",false);

        vc.print();

        env.execute();


    }
}
