package csv

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

import scala.util.Try

object CsvUtils {
  // Inspired by scala-csv (https://github.com/tototoshi/scala-csv) [Apache 2.0 LICENSE]
  private def splitCSVLine(line: String, delimiter: Char = ',', quote: Char = '"'): ArrayBuffer[String] = {
    val field = new StringBuilder()
    val parsed = new ArrayBuffer[String]()

    val chars = line.toCharArray
    var pos = 0
    val len = chars.length

    val FieldState           = 0
    val PlainFieldState      = 1
    val QuotedFieldState     = 2
    val QuotedFieldStateMain = 3
    val DelimiterState       = 4
    val EndState             = 5
    val ErrorState           = 6

    var state = FieldState
    var error = ""

    while (!(state == EndState || state == ErrorState)) {
      state match {
        case DelimiterState =>
          val curr = chars(pos)
          if (curr != delimiter) {
            error = s"Expecting a <$delimiter> at position $pos in string <$line>. Found a <$curr> instead"
            state = ErrorState
          } else {
            parsed += field.toString()
            field.clear()
            pos += 1
            state = FieldState
          }
        case FieldState =>
          if (pos >= len) state = EndState
          else if (chars(pos) == quote) state = QuotedFieldState
          else state = PlainFieldState
        case PlainFieldState =>
          if (pos >= len) state = EndState
          else if (chars(pos) == delimiter) state = DelimiterState
          else {
            field += chars(pos)
            pos += 1
          }
        case QuotedFieldState =>
          val isEmpty = pos+2 < len && chars(pos) == quote && chars(pos+1) == quote && chars(pos+2) == delimiter
          if (isEmpty) {pos += 2; state = DelimiterState} // an empty quoted field mini state
          else {pos += 1; state = QuotedFieldStateMain}   // skipping the opening quote
        case QuotedFieldStateMain =>
          if (pos >= len) {
            val fieldNum = parsed.length + 1
            error = s"Reached the end of the string <$line> while looking for the closing quote <$quote> of field " +
                    s"#$fieldNum"
            state = ErrorState
          } else {
            val curr = chars(pos)
            if (curr == quote) {
              if (pos+1 < len && chars(pos+1) == quote) { // we have 2 quote symbols and one is escaping the other
                field += quote
                pos += 2
              } else {                                    // this is the closing quote
                pos += 1
                if (pos == len) state = EndState
                else state = DelimiterState
              }
            } else {                                      // just a regular symbol.
              field += curr
              pos += 1
            }
          }
      }
    }

    state match {
      case EndState => parsed += field.toString()
      case ErrorState => throw new MalformedCSVException(error)
    }

    parsed
  }

  // Extracted from spark-csv (https://github.com/databricks/spark-csv) [Apache 2.0 LICENSE]
  // Specifically from the file /src/main/scala/com/databricks/spark/csv/util/TypeCast.scala
  // (https://github.com/databricks/spark-csv/blob/master/src/main/scala/com/databricks/spark/csv/util/TypeCast.scala)
  private def castTo(datum: String, castType: DataType): Any = {
    castType match {
      case _: ShortType   => datum.toShort
      case _: IntegerType => datum.toInt
      case _: LongType    => datum.toLong
      case _: FloatType   => datum.toFloat
      case _: DoubleType  => datum.toDouble
      case _: StringType  => datum
      case _ => throw new IllegalArgumentException(s"Unsupported type: ${castType.typeName}")
    }
  }

  // Includes fragments from spark-csv (https://github.com/databricks/spark-csv) [Apache 2.0 LICENSE]
  // Specifically from buildScan method from the file /src/main/scala/com/databricks/spark/csv/CsvRelation.scala
  // (https://github.com/databricks/spark-csv/blob/master/src/main/scala/com/databricks/spark/csv/CsvRelation.scala)
  private def parseCSVLine(line: String, schema: StructType, delimiter: Char = ',', quote: Char = '"',
                           nullValue: String = "NA"): Row = {
    val csvFields = splitCSVLine(line, delimiter, quote)
    val schemaFields = schema.fields

    if (csvFields.length != schemaFields.length) {
      val expected = schemaFields.length
      val found = csvFields.length
      throw new MalformedCSVException(s"Found $found fields (expecting $expected according to schema) in line <$line>")
    }

    val row = csvFields.zip(schemaFields).map{
      case (csvField, schemaField) => if (csvField == nullValue) null else castTo(csvField, schemaField.dataType)
    }

    Row.fromSeq(row)
  }

  // Includes fragments from spark-csv (https://github.com/databricks/spark-csv) [Apache 2.0 LICENSE]
  // Specifically from buildScan method from the file /src/main/scala/com/databricks/spark/csv/CsvRelation.scala
  // (https://github.com/databricks/spark-csv/blob/master/src/main/scala/com/databricks/spark/csv/CsvRelation.scala)
  def datframeFromCSV[T](rdd: RDD[String], schema: StructType, delimiter: Char = ',', quote: Char = '"',
                         parseMode: String = "DROPMALFORMED", nullValue: String = "NA"): DataFrame = {
    val rowRDD = parseMode.toUpperCase() match {
      case "FAILFAST" => rdd.map(line => parseCSVLine(line, schema, delimiter, quote, nullValue))
      case "DROPMALFORMED" => rdd.flatMap(line => Try{parseCSVLine(line, schema, delimiter, quote, nullValue)}.toOption)
      case _ => throw new IllegalArgumentException(s"""Unknown parse mode: \"$parseMode\"""")
    }

    SparkSession.builder().getOrCreate().createDataFrame(rowRDD, schema)
  }

  def datasetFromCSV[T](rdd: RDD[String], encoder: Encoder[T], delimiter: Char = ',', quote: Char = '"',
                        parseMode: String = "DROPMALFORMED", nullValue: String = "NA"): Dataset[T] = {
    val dataframe = datframeFromCSV(rdd, encoder.schema, delimiter, quote, parseMode, nullValue)
    dataframe.as(encoder)
  }
}
