package org.apache.spark.sql.arctern

import org.apache.spark.sql.arctern.functions._
import org.apache.spark.sql.arctern.index.RTreeIndex
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable

sealed abstract class SpatialJoinOperator {
  val name: String
}

object WithinOp extends SpatialJoinOperator {
  override final val name: String = "ST_Within"
}

object ContainsOp extends SpatialJoinOperator {
  override final val name: String = "ST_Contains"
}


object SpatialJoin {
  def apply(spark: SparkSession,
            left: DataFrame,
            right: DataFrame,
            leftGeomName: String,
            rightGeomName: String,
            joinType: JoinType = Inner,
            operator: SpatialJoinOperator = WithinOp,
            leftSuffix: String = "_left",
            rightSuffix: String = "_right"
           ): DataFrame = {
    // TODO: implement LeftOuter, RightOuter, FullOuter
    assert(joinType == Inner)
    // TODO: implement ContainsOp
    assert(operator == WithinOp)

    assert(left.columns.contains(leftGeomName))
    assert(right.columns.contains(rightGeomName))

    val leftNames = left.columns
    val rightNames = right.columns
    val renames = leftNames.toSet.intersect(rightNames.toSet)
    val allNames = (leftNames ++ rightNames).toSeq

    if (renames.nonEmpty) {
      def suffixColumn(suffix: String)(name: String) =
        if (renames.contains(name)) {
          col(name).as(name + suffix)
        } else {
          col(name)
        }

      def suffixName(suffix: String)(name: String) = {
        if (renames.contains(name)) {
          val newName = name + suffix
          if (allNames.contains(newName)) {
            throw new IllegalArgumentException(s""""${newName}" conflicts with pre-existing column name""")
          }
          newName
        } else {
          name
        }
      }

      val leftCols = leftNames.map(suffixColumn(leftSuffix))
      val rightCols = rightNames.map(suffixColumn(rightSuffix))

      val leftNew = left.select(leftCols: _*)
      val leftGeomNameNew = suffixName(leftSuffix)(leftGeomName)
      val rightNew = right.select(rightCols: _*)
      val rightGeomNameNew = suffixName(rightSuffix)(rightGeomName)

      join(spark, leftNew, rightNew, leftGeomNameNew, rightGeomNameNew)
    } else {
      join(spark, left, right, leftGeomName, rightGeomName)
    }
  }

  private def join(spark: SparkSession,
                   left: DataFrame,
                   right: DataFrame,
                   leftGeomName: String,
                   rightGeomName: String
                  ): DataFrame = {

    import spark.implicits._

    val TEMP_PREFIX = "arctern_temp__"
    val leftIndex = col(TEMP_PREFIX + "left_index")
    val rightIndex = col(TEMP_PREFIX + "right_index")

    val points = left.withColumn(leftIndex.toString(), monotonically_increasing_id())
    val polygons = right.withColumn(rightIndex.toString(), monotonically_increasing_id())

    val index_data = polygons.select(rightIndex,
      st_envelopinternal(col(rightGeomName)).as("envs"))

    val tree = new RTreeIndex

    index_data.collect().foreach {
      row => {
        val id = row.getAs[Long](0)
        val data = row.getAs[mutable.WrappedArray[Double]](1)
        val env = new Envelope(data(0), data(2), data(1), data(3))
        tree.insert(env, id)
      }
    }

    val broadcast = spark.sparkContext.broadcast(tree)

    val cross = points.select(leftIndex, col(leftGeomName)).as[(Long, Geometry)].flatMap {
      tp => {
        val (attr, point) = tp
        val env = point.getEnvelopeInternal
        val polyList = broadcast.value.query(env)
        polyList.toArray.map(poly => (poly.asInstanceOf[Long], attr, point))
      }
    }.withColumnRenamed("_1", rightIndex.toString())
      .withColumnRenamed("_2", leftIndex.toString())
      .withColumnRenamed("_3", leftGeomName)

    val joinResult = cross
      .join(polygons, rightIndex.toString())
      .filter(st_within(col(leftGeomName), col(rightGeomName)))
      .drop(leftGeomName).drop(rightIndex)

    val joinResult2 = points
      .join(joinResult, leftIndex.toString())
      .drop(leftIndex)

    joinResult2
  }
}
