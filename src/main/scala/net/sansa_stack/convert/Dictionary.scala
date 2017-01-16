package net.sansa_stack.convert

import nju.cichlid.Rules
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author Lorenz Buehmann
  */
object Dictionary {

  val conf = new SparkConf().setAppName("Dictionary Converter").setMaster("local[4]")
  val sc = new SparkContext(conf)

  def generateDictionary(triplesPath: String, dictionaryPath: String): Unit = {
    generateDictionary(loadTriples(triplesPath), dictionaryPath)
  }

  def generateDictionary(triples: RDD[(String, String, String)], dictionaryPath: String): Unit = {
    triples
      .flatMap(t => List(t._1, t._2, t._3)).distinct() // distinct RDF nodes
      .map(node => (Rules.getHashValue(node), node)) // tuples (md5(node) -> node)
      .saveAsObjectFile(dictionaryPath)
  }

  def decode(encodedTriplesPath: String, decodedTriplesPath: String, dictionaryPath: String): Unit = {
    // load dictionary
    val dictionary = loadDictionary(dictionaryPath).map(e => e._2 -> e._1)

    // load encoded triples
    val encodedTriples = loadTriples(encodedTriplesPath)

    // encode triples
    val triples = encodedTriples
      .map(t => (t._1, (t._2, t._3))).join(dictionary)
      .map(e => (e._2._1._1 , (e._2._2 , e._2._1._2))).join(dictionary)
      .map(e => (e._2._1._2, (e._2._1._1, e._2._2))).join(dictionary)
      .map(e => (e._2._1._1, e._2._1._2, e._2._2))

    triples.saveAsObjectFile(decodedTriplesPath)
  }

  def decode(encodedTriple: (String, String, String)): Unit = {

  }

  def encode(triplesPath: String, encodedTriplesPath: String, dictionaryPath: String): Unit = {
    // load dictionary
    val dictionary = loadDictionary(dictionaryPath).map(e => e._2 -> e._1)

    // load triples
    val triples = loadTriples(triplesPath)

    // encode triples
    val encodedTriples = triples
      .map(t => (t._1, (t._2, t._3))).join(dictionary)
      .map(e => (e._2._1._1 , (e._2._2 , e._2._1._2))).join(dictionary)
      .map(e => (e._2._1._2, (e._2._1._1, e._2._2))).join(dictionary)
      .map(e => (e._2._1._1, e._2._1._2, e._2._2))

    encodedTriples.saveAsObjectFile(encodedTriplesPath)

  }

  def encode(triple: (String, String, String)): Unit = {

  }

  def loadDictionary(dictionaryPath: String): RDD[(Seq[Byte], String)] = {
    sc.objectFile[(Seq[Byte], String)](dictionaryPath)
  }

  def loadTriples(triplesPath: String): RDD[(String, String, String)] = {
    sc
      .textFile(triplesPath)
      .map(line => line.split(" "))
      .map(tokens => (tokens(0), tokens(1), tokens(2)))
  }

  def main(args: Array[String]): Unit = {
    val dictionaryPath = "/tmp/lubm/dictionary"
    val encodedTriplesPath = "/tmp/lubm/encoded"
    val decodedTriplesPath = "/tmp/lubm/Universities.nt"

    generateDictionary(decodedTriplesPath, dictionaryPath)

    encode(decodedTriplesPath, encodedTriplesPath, dictionaryPath)
  }

}
