package com.youzhu.pre9;

import com.youzhu.bean.OrderEvent;
import com.youzhu.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink02_Practice_OrderReceiptWithJoin {

    public static void main(String[] args) throws Exception {

        /*
        不使用intervaljoin 原因  相当于 innerjoin  状态使用的   fullout join leftjoin rightjoin  都得使用connector
         */

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("input/ReceiptLog.csv");

        //转换为JavaBean
        //提取数据中的时间戳生成Watermark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });
        WatermarkStrategy<TxEvent> txEventWatermarkStrategy = WatermarkStrategy.<TxEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
            @Override
            public long extractTimestamp(TxEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });

        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = orderStreamDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");

                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));

                if ("pay".equals(orderEvent.getEventType())) {
                    out.collect(orderEvent);
                }
            }
        }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        SingleOutputStreamOperator<TxEvent> txEventSingleOutputStreamOperator = receiptStreamDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {

                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(txEventWatermarkStrategy);

        //连接支付流和到账流
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventSingleOutputStreamOperator.keyBy(OrderEvent::getTxId)
                .intervalJoin(txEventSingleOutputStreamOperator.keyBy(TxEvent::getTxId))
                .between(Time.seconds(-5), Time.seconds(10))
                //默认是左闭右闭  加以下俩参数  可以确定左不包含或右不包含
                //.lowerBoundExclusive()

                //.upperBoundExclusive()
                .process(new PayReciptJoinProcessFunc());

        //打印数据
        result.print();

        //执行
        env.execute();

    }
    public static class PayReciptJoinProcessFunc extends ProcessJoinFunction<OrderEvent,TxEvent, Tuple2<OrderEvent,TxEvent>>{
        @Override
        public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            out.collect(new Tuple2<>(left,right));

        }
    }
}


