package com.youzhu.pre3;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink09_Sink_Es {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //端口数据并转换为JavaBean
        //无界流
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999)
                //有界流   会自己进行主动关闭 会将所有内存中的数据进行flush提交
     //   SingleOutputStreamOperator<WaterSensor> streamOperator = env.readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将数据写入ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("pre1",9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts, new MyEsSink());
        //无界流 来一条处理一条  但是效率很低  批量提交参数设置
        waterSensorBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor> build = waterSensorBuilder.build();

        streamOperator.addSink(build);

        env.execute();
    }
    public static class MyEsSink implements ElasticsearchSinkFunction<WaterSensor>{
        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {

            HashMap<String, String> source = new HashMap<>();
            source.put("ts",element.getTs().toString());
            source.put("vc",element.getVc().toString());

            //创建Index请求
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor1")
                    .type("_doc")
                    .id(element.getId())
                    .source(element);
            indexer.add(indexRequest);
        }
    }
}
