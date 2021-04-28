package com.youzhu.pre6;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink08_State_Backend {


    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //定义状态后端  ,保存状态的位置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://pre1:8020"));
        //状态后端要导入依赖
        env.setStateBackend(new RocksDBStateBackend("hdfs://"));
        //开启checkpoint 状态后端和checkpoint结合使用
        env.getCheckpointConfig().enableUnalignedCheckpoints();

    }
}
