package com.youzhu.pre2;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink10_Transform_RichFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");
        /*
        过滤数据只取水位高于30的
         */
        SingleOutputStreamOperator<String> filter = stringDataStreamSource.filter(new MyRichFilter());

        filter.print();

        env.execute();
    }

    public static class MyRichFilter extends RichFilterFunction<String>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Integer.parseInt(split[2]) > 30;
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
