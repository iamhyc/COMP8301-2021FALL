/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.Comparator

import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext, Partitioner}
import org.apache.spark.RangePartitioner

object TeraSort {

  implicit val caseInsensitiveOrdering : Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator()

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Usage:")
      println("bin/spark-submit " +
        "--spark configuration" +
        "--class TeraSort " +
        "target/terasort-*.jar " +
        "[HDFSInputPath] [HDFSOutputPath]")
      System.exit(0)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("TeraSort")
    val sc = new SparkContext()

    val inputFiles = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputPath)
    val partitioner = new RangePartitioner(sc.defaultParallelism, inputFiles, true)
    val sortedRDD = inputFiles.repartitionAndSortWithinPartitions(partitioner)
    sortedRDD.saveAsNewAPIHadoopFile[TeraOutputFormat](outputPath)
    sc.stop()
  }

}
// scalastyle:on println
