package net.sansa_stack.convert

import java.io.File

import nju.cichlid.Rules
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Lorenz Buehmann
  */
object Dictionary {

  def generateDictionary(implicit sc: SparkContext, triplesPath: String, dictionaryPath: String): Unit = {
    generateDictionary(loadTriples(sc, triplesPath), dictionaryPath)
  }

  def generateDictionary(triples: RDD[(String, String, String)], dictionaryPath: String): Unit = {
    triples
      .flatMap(t => List(t._1, t._2, t._3)).distinct() // distinct RDF nodes
      .map(node => (Rules.getHashValue(node), node)) // tuples (md5(node) -> node)
      .saveAsObjectFile(dictionaryPath)
  }

  def decode(implicit sc: SparkContext, encodedTriplesPath: String, decodedTriplesPath: String, dictionaryPath: Option[File]): Unit = {
    // load dictionary
    val dictionary = loadDictionary(sc, dictionaryPath.get.getPath)

    // load encoded triples
    val encodedTriples = loadEncodedTriples(sc, encodedTriplesPath)

    // decode triples
    val triples = decode(encodedTriples, dictionary)

    // save as N-Triples
    triples.map(t => t._1 + " " + t._2 + " " + t._3 + " .").saveAsTextFile(decodedTriplesPath)
  }

  def decode(encodedTriples: RDD[(Seq[Byte], Seq[Byte], Seq[Byte])], dictionary: RDD[(Seq[Byte], String)]): RDD[(String, String, String)] = {
    encodedTriples
      .map(t => (t._1, (t._2, t._3))).join(dictionary)
      .map(e => (e._2._1._1 , (e._2._2 , e._2._1._2))).join(dictionary)
      .map(e => (e._2._1._2, (e._2._1._1, e._2._2))).join(dictionary)
      .map(e => (e._2._1._1, e._2._1._2, e._2._2))
  }

  def encode(implicit sc: SparkContext, triplesPath: String, encodedTriplesPath: String, dictionaryPath: Option[File]): Unit = {
    // load triples
    val triples = loadTriples(sc, triplesPath)

    // encode triples
    val encodedTriples =
    dictionaryPath match {
      case Some(path) => {
        // load dictionary
        val dictionary = loadDictionary(sc, path.getPath).map(e => e._2 -> e._1)

        encode(sc, triples, dictionary)
      }
      case None => encode(sc, triples)
    }

    encodedTriples.saveAsObjectFile(encodedTriplesPath)
  }

  def encode(implicit sc: SparkContext, triples: RDD[(String, String, String)], dictionary: RDD[(String, Seq[Byte])]): RDD[(Seq[Byte], Seq[Byte], Seq[Byte])] = {
    triples
      .map(t => (t._1, (t._2, t._3))).join(dictionary)
      .map(e => (e._2._1._1 , (e._2._2 , e._2._1._2))).join(dictionary)
      .map(e => (e._2._1._2, (e._2._1._1, e._2._2))).join(dictionary)
      .map(e => (e._2._1._1, e._2._1._2, e._2._2))
  }

  def encode(implicit sc: SparkContext, triples: RDD[(String, String, String)]): RDD[(Seq[Byte], Seq[Byte], Seq[Byte])] = {
    triples.map(t => (Rules.getHashValue(t._1), Rules.getHashValue(t._2), Rules.getHashValue(t._3)))
  }

  def loadDictionary(implicit sc: SparkContext, dictionaryPath: String): RDD[(Seq[Byte], String)] = {
    sc.objectFile[(Seq[Byte], String)](dictionaryPath)
  }

  def loadTriples(implicit sc: SparkContext, triplesPath: String): RDD[(String, String, String)] = {
    sc
      .textFile(triplesPath)
      .map(line => line.split(" "))
      .map(tokens => (tokens(0), tokens(1), tokens(2)))
  }

  def loadEncodedTriples(implicit sc: SparkContext, encodedTriplesPath: String): RDD[(Seq[Byte], Seq[Byte], Seq[Byte])] = {
    sc.objectFile[(Seq[Byte], Seq[Byte], Seq[Byte])](encodedTriplesPath)
  }

  import java.io.File

  case class Config(triplesPath: Seq[File] = Seq(new File(".")),
                    encodedTriplesPath: Seq[File] = Seq(new File(".")),
                    dictionaryPath: Option[File] = Option.empty,
                    mode: String = "")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Dictionary Converter")
      .setMaster("local[4]")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val parser = new scopt.OptionParser[Config]("dictionary") {
      head("dictionary", "v0.1")

      cmd("generate").action((_, c) => c.copy(mode = "generate")).
        text("generate a dictionary for a given triples file.").
        children(
          opt[Seq[File]]("source").abbr("s").required().action((x, c) =>
            c.copy(triplesPath = x)).text("the N-Triples file"),
          opt[File]("target").abbr("t").required().action((x, c) =>
            c.copy(dictionaryPath = Some(x))).text("the path of the dictionary")
        )

      cmd("encode").action((_, c) => c.copy(mode = "encode")).
        text("encode an N-Triples file based on a given dictionary.").
        children(
          opt[Seq[File]]("source").abbr("s").required().action((x, c) =>
            c.copy(triplesPath = x)).text("the N-Triples file"),
          opt[File]("target").abbr("t").required().action((x, c) =>
            c.copy(encodedTriplesPath = Seq(x))).text("the output path of the encoded N-Triples file"),
          opt[File]("dict").abbr("d").optional().action((x, c) =>
            c.copy(dictionaryPath = Some(x))).text("the path of the dictionary (if exist)")
        )

      cmd("decode").action((_, c) => c.copy(mode = "decode")).
        text("decodes an encoded N-Triples file based on a given dictionary.").
        children(
          opt[Seq[File]]("source").abbr("s").required().action((x, c) =>
            c.copy(encodedTriplesPath = x)).text("the encoded N-Triples file"),
          opt[File]("target").abbr("t").required().action((x, c) =>
            c.copy(triplesPath = Seq(x))).text("the output path of the decoded N-Triples file"),
          opt[File]("dict").abbr("d").required().action((x, c) =>
            c.copy(dictionaryPath = Some(x))).text("the path of the dictionary")
        )
    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        config.mode match {
          case "generate" => generateDictionary(sc, config.triplesPath.map(file => file.getPath).mkString(","), config.dictionaryPath.get.getPath)
          case "encode" => encode(sc, config.triplesPath.map(file => file.getPath).mkString(","), config.encodedTriplesPath.map(file => file.getPath).mkString(","), config.dictionaryPath)
          case "decode" => decode(sc, config.encodedTriplesPath.map(file => file.getPath).mkString(","), config.triplesPath.map(file => file.getPath).mkString(","), config.dictionaryPath)
        }
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

}
