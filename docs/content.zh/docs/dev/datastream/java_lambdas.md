---
title: "Java Lambda Expressions"
weight: 300
type: docs
aliases:
  - /zh/dev/java_lambdas.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Java Lambda 表达式

Java 8 引入了几种新的语言特性，旨在实现更快、更清晰的编码。作为最重要的特性，即所谓的“Lambda 表达式”，它开启了函数式编程的大门。Lambda 表达式允许以简捷的方式实现和传递函数，而无需声明额外的（匿名）类。

{{< hint info >}}
Flink 支持对 Java API 的所有算子使用 Lambda 表达式，但是，当 Lambda 表达式使用 Java 泛型时，你需要 *显式* 地声明类型信息。
{{< /hint >}}

本文档介绍如何使用 Lambda 表达式并描述了其（Lambda 表达式）当前的限制。有关 Flink API 的通用介绍，请参阅 [DataStream API 编程指南]({{< ref "docs/dev/datastream/overview" >}})。

## 示例和限制

下面的这个示例演示了如何实现一个简单的内联 `map()` 函数，它使用 Lambda 表达式计算输入值的平方。

不需要声明 `map()` 函数的输入 `i` 和输出参数的数据类型，因为 Java 编译器会对它们做出推断。

```java
env.fromElements(1, 2, 3)
// 返回 i 的平方
.map(i -> i*i)
.print();
```

由于 `OUT` 是 `Integer` 而不是泛型，所以 Flink 可以从方法签名 `OUT map(IN value)` 的实现中自动提取出结果的类型信息。

不幸的是，像 `flatMap()` 这样的函数，它的签名 `void flatMap(IN value, Collector<OUT> out)` 被 Java 编译器编译为 `void flatMap(IN value, Collector out)`。这样 Flink 就无法自动推断输出的类型信息了。

Flink 很可能抛出如下异常：

```
org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing.
    In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved.
    An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface.
    Otherwise the type has to be specified explicitly using type information.
```

在这种情况下，需要 *显式* 指定类型信息，否则输出将被视为 `Object` 类型，这会导致低效的序列化。

```java
DataStream<Integer> input = env.fromElements(1, 2, 3);

// 必须声明 collector 类型
input.flatMap((Integer number, Collector<String> out) -> {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < number; i++) {
        builder.append("a");
        out.collect(builder.toString());
    }
})
// 显式提供类型信息
.returns(Types.STRING)
// 打印 "a", "a", "aa", "a", "aa", "aaa"
.print();
```

当使用 `map()` 函数返回泛型类型的时候也会发生类似的问题。下面示例中的方法签名 `Tuple2<Integer, Integer> map(Integer value)` 被擦除为 `Tuple2 map(Integer value)`。

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

env.fromElements(1, 2, 3)
    .map(i -> Tuple2.of(i, i))    // 没有关于 Tuple2 字段的信息
    .print();
```

一般来说，这些问题可以通过多种方式解决：

```java
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

// 使用显式的 ".returns(...)"
env.fromElements(1, 2, 3)
    .map(i -> Tuple2.of(i, i))
    .returns(Types.TUPLE(Types.INT, Types.INT))
    .print();

// 使用类来替代
env.fromElements(1, 2, 3)
    .map(new MyTuple2Mapper())
    .print();

public static class MyTuple2Mapper extends MapFunction<Integer, Tuple2<Integer, Integer>> {
    @Override
    public Tuple2<Integer, Integer> map(Integer i) {
        return Tuple2.of(i, i);
    }
}

// 使用匿名类来替代
env.fromElements(1, 2, 3)
    .map(new MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer i) {
            return Tuple2.of(i, i);
        }
    })
    .print();

// 也可以像这个示例中使用 Tuple 的子类来替代
env.fromElements(1, 2, 3)
    .map(i -> new DoubleTuple(i, i))
    .print();

public static class DoubleTuple extends Tuple2<Integer, Integer> {
    public DoubleTuple(int f0, int f1) {
        this.f0 = f0;
        this.f1 = f1;
    }
}
```

{{< top >}}
