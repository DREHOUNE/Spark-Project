package services

import domain.DomainEncoders._
import domain.{Agency, EdfS, Installation, Region, Team}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe._

object ServiceEdf {
  def joinInstallationTeam(installations: Dataset[Installation], teams: Dataset[Team], agences: Dataset[Agency],
                           regions: Dataset[Region]) = {

    val instalTeam = installations
      .join(teams,installations("codeEquipe") === teams("code"),"left")

    val instalTeamAgence = instalTeam
      .join(agences,instalTeam("codeAgence") === agences("code"),"left")

    instalTeamAgence
      .join(regions,instalTeamAgence("codeRegion") === regions("code"),"left")
      .map(elt => EdfS( elt.getInt(1), elt.getString(4), elt.getString(3), elt.getString(2),
        elt.getString(0), elt.getString(7), elt.getString(5), elt.getString(10),
        elt.getString(9), elt.getString(12)))
  }


  def readDataSet[T: TypeTag](inputfile: String)(implicit spark: SparkSession): Dataset[T] = {

    implicit val encoder = ExpressionEncoder[T]

    spark.read.json(inputfile).flatMap(_.copy(): Option[T])
  }

}

