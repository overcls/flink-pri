package com.youzhu.pre2;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_RichMap {
    /*
    用richmapfunction主要是用来连接第三方库  因为一个生命周期连接一次
    普通mapfunction也可以用  连接次数过多
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
        open方法 一个并行度调用一次
        close方法 一个并行度调用俩次  自身cancel方法一次  所有的close方法一次
        7*24 job  close方法是不会被调用的
         */
        env.setParallelism(2);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(new MyRichMap());

        waterSensorDS.print();

        env.execute();


    }
    /*
    多了 生命周期open close 可以将数据库连接写在生命周期里  可获得运行时上下文执行环境  获取状态  状态编程
     */
    public static class MyRichMap extends RichMapFunction<String, WaterSensor> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            //状态编程
            //getRuntimeContext();

                String[] split = value.split(",");

                return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
            }


        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
