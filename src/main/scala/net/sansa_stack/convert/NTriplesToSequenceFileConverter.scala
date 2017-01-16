package net.sansa_stack.convert

import nju.cichlid.Rules
import org.apache.hadoop.io.{BytesWritable, IntWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Convert an N-Triples file to a Hadoop sequence file
  *
  * @author Lorenz Buehmann
  */
object NTriplesToSequenceFileConverter {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("N-Triples File to Sequence File Converter").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)

    // load triples RDD
    val triples = sc
      .textFile(input)
      .map(line => line.split(" "))
      .map(tokens => (tokens(0), tokens(1), tokens(2)))

    // encode triples
    val encodedTriples: RDD[(BytesWritable, IntWritable)] =
      triples.map(t =>
        (
          new BytesWritable((Rules.getHashValue(t._1) ++ Rules.getHashValue(t._2) ++ Rules.getHashValue(t._3)).toArray),
          new IntWritable(1)
        )
      )

    // save as sequence file
    encodedTriples.saveAsSequenceFile(output)

  }


}
