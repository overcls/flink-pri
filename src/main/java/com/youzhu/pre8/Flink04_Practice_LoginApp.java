package com.youzhu.pre8;

import com.youzhu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

public class Flink04_Practice_LoginApp {
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

        //使用ProcessAPI，状态，定时器  2s内失败2次
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginKeyedProcessFunc(2, 2));

        //打印结果
        result.print();

        //执行任务
        env.execute();
    }

    public static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {

        //定义属性信息
        private Integer ts;
        private Integer count;

        public LoginKeyedProcessFunc(Integer ts, Integer count) {
            this.ts = ts;
            this.count = count;
        }

        //生命状态
        private ListState<LoginEvent> loginEventListState;
        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            //取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            Long timerTs = valueState.value();

            //去除事件类型
            String eventType = value.getEventType();

            //判断是否为第一条失败数据，则需要注册定时器
            if ("fail".equals(eventType)) {

                if (!iterator.hasNext()) {   //为第一条失败数据
                    //注册定时器
                    long curTs = ctx.timerService().currentWatermark() + ts * 1000L;
                    ctx.timerService().registerEventTimeTimer(curTs);
                    //更新时间状态
                    valueState.update(curTs);
                }

                //将当前的失败数据加入状态
                loginEventListState.add(value);
            } else {

                //不等于null说明已经注册过定时器
                if (timerTs != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTs);

                }

                //成功数据，清空List并删除定时器
                loginEventListState.clear();
                valueState.clear();

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);

            //判断连续失败的次数
            if (loginEvents.size() >= count) {

                LoginEvent first = loginEvents.get(0);
                LoginEvent last = loginEvents.get(loginEvents.size() - 1);
                out.collect(first.getUserId() +
                        "用户在" +
                        first.getEventTime() +
                        "--" +
                        last.getEventTime() +
                        "之间，连续登录失败了" +
                        loginEvents.size() +
                        "次！");
            }
            //清空状态
            loginEventListState.clear();
            valueState.clear();
        }
    }
}
