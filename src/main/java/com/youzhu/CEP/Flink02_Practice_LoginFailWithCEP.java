package com.youzhu.CEP;

import com.youzhu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink02_Practice_LoginFailWithCEP {
    /*
    如果同一用户（可以是不同IP）在2秒内连续俩次登陆失败，就认为存在恶意登录的风险，输出相关的信息进行报错提示，这是所有网站风控的基本一环
     */

    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文本数据 转换为JavaBean提取时间戳 生成Watermark
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        //按照用户ID分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //使用CEP编程实现  2秒内连续登陆两次报警  定义模式序列 使用循环模式
        Pattern<LoginEvent, LoginEvent> loginEventLoginEventPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
                /*
                使用 time  默认使用的是followby  宽松近邻加参数 .consecutive()指定使用严格近邻
                 */
            }
        }).times(2)
                .consecutive()
                .within(Time.seconds(2));

        //将模式序列作用在流上面   CEP编程可以处理乱序数据底层实现使用的是开窗 和状态编程
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, loginEventLoginEventPattern);

        //超时事件对于本需求来说不需要  但是可以获取
        //提取匹配上的事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginFailPatternSelectFunc());

        result.print();

        env.execute();

    }
    public static class LoginFailPatternSelectFunc implements PatternSelectFunction<LoginEvent,String>{
        @Override
        public String select(Map<String, List<LoginEvent>> map) throws Exception {

            //取出数据
            LoginEvent start = map.get("start").get(0);
            LoginEvent next = map.get("start").get(1);

            //输出结果
            return start.getUserId()+"在"+start.getEventTime()+"____---____"+next.getEventTime()+"之间,连续登陆失败2次";
        }
    }



}
