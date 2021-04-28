package com.youzhu.pre3;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class Flink08_Sink_Redis {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将数据写入Redis
        //集群模式(不同key散列到不同机器上)  单机模式 哨兵模式

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("pre1")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        streamOperator.addSink(new RedisSink<>(redisConfig,new MyredisMapper()))
;
        //执行任务
        env.execute();
    }
    public static class MyredisMapper implements RedisMapper<WaterSensor>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"Sensor");
        }

        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(WaterSensor data) {
            return data.getVc().toString();
        }
    }
}
