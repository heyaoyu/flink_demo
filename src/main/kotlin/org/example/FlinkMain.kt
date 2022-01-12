package org.example

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import java.util.*

// sudo /opt/flink-1.14.2/bin/start-cluster.sh
// sudo /opt/flink-1.14.2/bin/flink run ~/IdeaProjects/flink_demo/target/flink_demo-1.0-SNAPSHOT-jar-with-dependencies.jar
// cat /opt/flink-1.14.2/log/flink-root-taskexecutor-0-heyaoyudeMacBook-Pro.local.out
// sudo /opt/flink-1.14.2/bin/stop-cluster.sh

// <main.class>org.example.FlinkMainKt</main.class>

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val ds = env.fromCollection(listOf(Entity("one", 1), Entity("two", 2)))
    ds.print("ds")
    val autoSource = env.addSource(AutoSource(), "auto")
    autoSource.print("auto")
    val ks = autoSource.keyBy(object : KeySelector<Entity, String> {
        override fun getKey(entity: Entity): String = entity.name
    })
    ks.process(object : KeyedProcessFunction<String, Entity, Int>() {
        override fun processElement(entity: Entity?, context: Context?, collector: Collector<Int>?) {
            collector?.collect(entity?.value)
        }
    }).print()
    env.execute("entity")
}

data class Entity(var name: String, var value: Int)

val random = Random()

class AutoSource() : SourceFunction<Entity> {

    override fun run(context: SourceFunction.SourceContext<Entity>?) {
        for (i in 1..10) {
            val rdm = random.nextInt(10)
            context?.collect(Entity("auto_${rdm % 2}", rdm))
            Thread.sleep(1000)
        }
    }

    override fun cancel() {
    }
}