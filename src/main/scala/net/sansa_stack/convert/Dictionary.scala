package net.sansa_stack.convert

import nju.cichlid.Rules
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Lorenz Buehmann
  */
object Dictionary {

  val conf = new SparkConf()
    .setAppName("Dictionary Converter")
    .setMaster("local[4]")
    .set("spark.hadoop.validateOutputSpecs", "false")
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

  import java.io.File

  case class Config(triplesPath: File = new File("."),
                    encodedTriplesPath: File = new File("."),
                    dictionaryPath: File = new File("."),
                    mode: String = "")

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config]("dictionary") {
      head("dictionary", "v0.1")

      cmd("generate").action((_, c) => c.copy(mode = "generate")).
        text("generate a dictionary for a given triples file.").
        children(
          opt[File]("source").abbr("s").required().action((x, c) =>
            c.copy(triplesPath = x)).text("the N-Triples file"),
          opt[File]("target").abbr("t").required().action((x, c) =>
            c.copy(dictionaryPath = x)).text("the path of the dictionary")
        )

      cmd("encode").action((_, c) => c.copy(mode = "encode")).
        text("encode an N-Triples file based on a given dictionary.").
        children(
          opt[File]("source").abbr("s").required().action((x, c) =>
            c.copy(triplesPath = x)).text("the N-Triples file"),
          opt[File]("target").abbr("t").required().action((x, c) =>
            c.copy(encodedTriplesPath = x)).text("the output path of the encoded N-Triples file"),
          opt[File]("dict").abbr("d").required().action((x, c) =>
            c.copy(dictionaryPath = x)).text("the path of the dictionary")
        )

      cmd("decode").action((_, c) => c.copy(mode = "decode")).
        text("decodes an encoded N-Triples file based on a given dictionary.").
        children(
          opt[File]("source").abbr("s").required().action((x, c) =>
            c.copy(encodedTriplesPath = x)).text("the encoded N-Triples file"),
          opt[File]("target").abbr("t").required().action((x, c) =>
            c.copy(triplesPath = x)).text("the output path of the decoded N-Triples file"),
          opt[File]("dict").abbr("d").required().action((x, c) =>
            c.copy(dictionaryPath = x)).text("the path of the dictionary")
        )
    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        config.mode match {
          case "generate" => generateDictionary(config.triplesPath.getPath, config.dictionaryPath.getPath)
          case "encode" => encode(config.triplesPath.getPath, config.encodedTriplesPath.getPath, config.dictionaryPath.getPath)
          case "decode" => decode(config.encodedTriplesPath.getPath, config.triplesPath.getPath, config.dictionaryPath.getPath)
        }
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

}
