package net.sansa_stack.convert

import nju.cichlid.Rules
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
    val dictionary = loadDictionary(dictionaryPath)

    // load encoded triples
    val encodedTriples = loadEncodedTriples(encodedTriplesPath)

    // encode triples
    val triples = encodedTriples
      .map(t => (t._1, (t._2, t._3))).join(dictionary)
      .map(e => (e._2._1._1 , (e._2._2 , e._2._1._2))).join(dictionary)
      .map(e => (e._2._1._2, (e._2._1._1, e._2._2))).join(dictionary)
      .map(e => (e._2._1._1, e._2._1._2, e._2._2))
      .map(t => t._1 + " " + t._2 + " " + t._3 + " .")

    triples.saveAsTextFile(decodedTriplesPath)
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

  def loadEncodedTriples(encodedTriplesPath: String): RDD[(Seq[Byte], Seq[Byte], Seq[Byte])] = {
    sc.objectFile[(Seq[Byte], Seq[Byte], Seq[Byte])](encodedTriplesPath)
  }

  def main(args: Array[String]): Unit = {

    val source = "/tmp/lubm/Universities.nt"

    val dictionaryPath = "/tmp/lubm/dictionary"
    val encodedTriplesPath = "/tmp/lubm/encoded"
    val decodedTriplesPath = "/tmp/lubm/decoded"

//    generateDictionary(source, dictionaryPath)

    encode(source, encodedTriplesPath, dictionaryPath)

    decode(encodedTriplesPath, decodedTriplesPath, dictionaryPath)
  }

}
