package com.youzhu.CEP;

import com.youzhu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class Flink03_Practice_OrderPayWithCEP {
    /*
    订单支付实时监控 & 订单支付设置失效时间,超过一段时间不支付的订单就会被取消  断点处理文本  周期性处理流 读文本数据过快 会出现watermark出现跳变
     */

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文本数据创建流,转换成JavaBean同时提取时间戳生成WaterMark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        //按照OrderID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //定义模式序列
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.seconds(3));

        //将模式序列作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, orderEventPattern);

        //提取正常匹配上的以及超时事件
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("No Pay") {
        },
                new OrderPayTimeOutFunc(),
                new OrderPaySelectFunc());

        //打印
        result.print();
        result.getSideOutput(new OutputTag<String>("No Pay"){}).print("Time Out");

        //执行任务
        env.execute();

    }
    public static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent,String>{


        @Override
        public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {

                //提取事件
                OrderEvent createEvent = map.get("start").get(0);

                //输出结果  侧输出流

                return createEvent.getOrderId()+"在"+createEvent.getEventTime()+"创建订单,并在"+l / 1000+"超时";

        }
    }
    public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent,String> {


        @Override
        public String select(Map<String, List<OrderEvent>> map) throws Exception {
            //提取事件
            OrderEvent creatEvent = map.get("start").get(0);
            OrderEvent payEvent = map.get("follow").get(0);
            //输出结果  主流
            return creatEvent.getOrderId()+"在"+creatEvent.getEventTime()+"创建订单,并在"+payEvent.getEventTime()+"完成支付";
        }
    }

}
/*
public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            //提取事件
            OrderEvent creatEvent = map.get("start").get(0);
            OrderEvent payEvent = map.get("follow").get(0);
            //输出结果  主流
            return creatEvent.getOrderId()+"在"+creatEvent.getEventTime()+"创建订单,并在"+payEvent.getEventTime()+"完成支付";
        }
 */
