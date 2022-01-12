package org.example

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

// sudo /opt/flink-1.14.2/bin/start-cluster.sh
// sudo /opt/flink-1.14.2/bin/flink run ~/IdeaProjects/flink_demo/target/flink_demo-1.0-SNAPSHOT-jar-with-dependencies.jar
// cat /opt/flink-1.14.2/log/flink-root-taskexecutor-0-heyaoyudeMacBook-Pro.local.out
// sudo /opt/flink-1.14.2/bin/stop-cluster.sh

// <main.class>org.example.FlinkMainKt</main.class>

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val ds = env.fromCollection(listOf(Entity("one", 1), Entity("two", 2)))
    ds.print("ds")
    env.execute("entity")
}

data class Entity(var name: String, var value: Int)