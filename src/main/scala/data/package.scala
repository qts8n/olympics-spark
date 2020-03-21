import org.apache.spark.sql.{Encoder, Encoders}

package object data {
  case class Event(
    id: String,
    name: String,
    sex: String,
    age: java.lang.Short,
    height: java.lang.Short,
    weight: java.lang.Float,
    team: String,
    noc:  String,
    games: String,
    year: java.lang.Short,
    season: String,
    city: String,
    sport: String,
    event: String,
    medal: String
  )

  val eventEncoder: Encoder[Event] = Encoders.product[Event]
}
