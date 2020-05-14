import org.apache.spark.sql.{Encoder, Encoders}

object Implicits {
  implicit val encoder1 = Encoders.product[zip]
}
