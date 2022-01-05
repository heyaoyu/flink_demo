package org.example

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

// sudo bin/flink run ~/IdeaProjects/flink_demo/target/flink_demo-1.0-SNAPSHOT-jar-with-dependencies.jar
// cat log/flink-root-taskexecutor-0-heyaoyudeMacBook-Pro.local.out

// <main.class>org.example.FlinkMainKt</main.class>

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val ds = env.fromCollection(listOf(Entity("one", 1), Entity("two", 2)))
    ds.print("ds")
    env.execute()
}

data class Entity(var name: String, var value: Int)