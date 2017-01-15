package net.sansa_stack.convert

import nju.cichlid.Rules
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Convert an N-Triples file to a Hadoop sequence file
  *
  * @author Lorenz Buehmann
  */
object NTriplesToSequenceFileConverter {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("N-Triples File to Sequence File Converter").setMaster("localhost[4]")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)

    // load triples RDD
    val triples: RDD[(NullWritable, BytesWritable)] = sc
      .textFile(input)
      .map(line => (
        NullWritable.get(),
        new BytesWritable(
          line
            .split(" ")
            .map(token => Rules.getHashValue(token))
            .flatten
        )
      )
      )
    triples.saveAsSequenceFile(output)

  }
}
