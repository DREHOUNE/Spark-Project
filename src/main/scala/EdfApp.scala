import domain.{Agency, Installation, Region, Team}
import org.apache.spark.sql.{Dataset, SparkSession}
import services.ServiceEdf

case class A(name:String)
class B(name:String)
object  EdfApp {

  def main(args: Array[String]): Unit = {

    implicit val spark : SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MyApp.edf")
      //.config("spark.testing.memory", "2147480000")
      .getOrCreate()

    println("-----------------------------------Begin Program----------------------------------------- ")

    val pathI = "src/main/resources/installations.json"
    val pathT = "src/main/resources/equipes.json"
    val pathR = "src/main/resources/regions.json"
    val pathA = "src/main/resources/agences.json"

    val installationDS: Dataset[Installation] = ServiceEdf.readDataSet(pathI)
    val teamDS: Dataset[Team] = ServiceEdf.readDataSet(pathT)
    val regionDS: Dataset[Region] = ServiceEdf.readDataSet(pathR)
    val agenceDS: Dataset[Agency] = ServiceEdf.readDataSet(pathA)

    val installationHead = installationDS.head()
    val teamHead = teamDS.head()
    val regionHead = regionDS.head()
    val agenceHead = agenceDS.head()

    val installations = installationDS.filter( raw => raw != installationHead)
    val teams = teamDS.filter( raw => raw != teamHead)
    teams.show()
    val regions = regionDS.filter( raw => raw != regionHead)
    val agences = agenceDS.filter( raw => raw != agenceHead)

    val edfDs = ServiceEdf.joinInstallationTeam(installations,teams, agences, regions)
    edfDs.show()

    println("-----------------------------------End Program----------------------------------------- ")

  }


}
