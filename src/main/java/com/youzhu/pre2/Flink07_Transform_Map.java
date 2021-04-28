package com.youzhu.pre2;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Transform_Map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");

        stringDataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
            }
        })
                .print();

        env.execute();
    }
    public static class Mymap implements MapFunction<String,WaterSensor>{
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");

            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }
    }
}
