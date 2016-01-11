# FlintStone

---

Contact: [Chao Zhang](mailto:chao.c.zhang@intel.com), [Daoyuan Wang](mailto:daoyuan.wang@intel.com), [Hao Cheng](mailto:hao.cheng@intel.com), [Jiangang Duan](mailto: jiangang.duan@intel.com)

---
**FlintStone** is a project based on [Apache Spark](https://github.com/apache/spark)
and [Apache Calcite](https://github.com/apache/calcite), aiming to support
SQL standard on Spark. Our target is to advance the progress of Spark SQL by bridging the gap between SQL standard DML and current DML supported by Spark.

FlintStone provides:

1. SQL support that is closer to SQL Standard.
2. A Context that could use in your Spark program, as well as in thriftserver or `bin/spark-sql` CLI.

## Quick Start

To use FlintStone, you need to get the compiled jar file into every node of your cluster, and place them into extra classpath of your spark driver and executors. You can do this by adding the following two lines to your `conf/spark-defaults.conf`:

    spark.driver.extraClassPath        /path/to/jar/flintstone.jar
    spark.executor.extraClassPath      /path/to/jar/flintstone.jar

If you are using `bin/spark-sql`, you will see the following line in the output:

    SET spark.sql.dialect=com.intel.ssg.bdt.spark.sql.CalciteDialect

If you want to use the original parser of Spark, you can use the command:

    SET spark.sql.dialect=hiveql;

or

    SET spark.sql.dialect=sql;

To switch back to FlintStone, you only need to type:

    SET spark.sql.dialect=com.intel.ssg.bdt.spark.sql.CalciteDialect;

#### Use FlintStone in your Spark application

`FlintContext` is the main entry point for all FlintStone related functionalities. `FlintContext` is 100% compatible with `HiveContext`, and will be automatically created as default when `spark-defaults.conf` is properly set. Currently you cannot use FlintStone in `bin/spark-shell` with full FlintStone features, but when it comes to your own Spark application, FlintStone features are totally supported. You can create your `FlintContext` instance in the same way as `HiveContext`.

```scala
import org.apache.spark.sql.flint.FlintContext
import org.apache.spark.sql.flint.FlintContext._

val sc: SparkContext
val fc: FlintContext = new FlintContext(sc)
```

Or you could use `sql(text: String)` to leverage Calcite grammar support, like:

```scala
val df1 = sql("select trim(leading "NO_" from user_id) as a, user_age as age from t1")
val df2 = sql("select a, b from t2 where b2 like \"S%SS\" escape \"S\"")
df1.registerTempTable("tt1")
df2.registerTempTable("tt2")
val df3 = sql(select * from tt1 natural tt2)
df3.foreach(println)
```

#### Use FlintStone in your Spark Thriftserver or CliDriver

There are merely user experience gaps between `FlintContext` and `HiveContext`. The only thing that differs is FlintStone offers a better grammar support. 

## How to Build and Deploy

FlintStone is built with [maven](https://maven.apache.org/), you could use maven related commands to test/compile/package. To build FlintStone from source codes, just use `mvn clean -DskipTests package`.

Currently FlintStone can only run with Spark-1.5.2, more Spark versions will be supported in future.

To use FlintStone, you need to put the packaged jar into your driver and executors' extra classpath.

## Potential improvement

1. more grammar by updating our Calcite dependency version.
2. add support for more awesome features.

You are welcome to share your ideas and please feel free to propose an issue or pull request.

## License

This project is open sourced under Apache License Version 2.0.
