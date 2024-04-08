$FLINK_HOME/bin/flink run \
# 指定yarn的Per-job模式，-t等价于-Dexecution.target
-t yarn-per-job \
# yarn应用的自定义name
-Dyarn.application.name=flink-pre-job-word-count \
# 未指定并行度时的默认并行度值, 该值默认为1
-Dparallelism.default=3 \
# JobManager进程的内存
-Djobmanager.memory.process.size=2048mb \
# TaskManager进程的内存
-Dtaskmanager.memory.process.size=2048mb \
# 每个TaskManager的slot数目, 最佳配比是和vCores保持一致
-Dtaskmanager.numberOfTaskSlots=2 \
# 防止日志中文乱码
-Denv.java.opts="-Dfile.encoding=UTF-8" \
# 支持火焰图, Flink1.13新特性, 默认为false, 开发和测试环境可以开启, 生产环境建议关闭
-Drest.flamegraph.enabled=true \
# 入口类
-c xxx.xxx.WordCount \
# 提交Job的jar包
xxx.xxx.WordCount.jar