package org.apache.spark.ml.linalg

import com.tfedorov.text.VocabFreq
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by Taras_Fedorov on 2/25/2017.
  */

class VocabFreqTransformer(stopWords: Seq[String]) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  override def transform(targetDS: Dataset[_]): DataFrame = {

    val changedUDF = udf { document: Seq[String] => Vectors.dense(VocabFreq(document, stopWords.toSet).toArray) }

    import org.apache.spark.sql

    targetDS.withColumn("features", changedUDF(col("tokens")))

  }

  override def copy(extra: ParamMap): Transformer

  = ???

  override def transformSchema(schema: StructType): StructType

  = {
    SchemaUtils.appendColumn(schema, "features", new VectorUDT(), false)
  }

  override val uid: String =
    "1234"

}
