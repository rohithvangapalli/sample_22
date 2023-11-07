import scala.util.{Failure, Success, Try}

Try {
  import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
  import com.github.music.of.the.ainur.almaren.Almaren
  import com.modak.common.credential.Credential
  import com.modak.common._

  //val args = sc.getConf.get("spark.driver.args").split("\\s+")
  val args = sc.getConf.get("spark.driver.args").split(",")
  val token = sc.getConf.get("spark.nabu.token")
  val cred_id = args(0).toInt
  val cred_type = args(1).toInt
  val endpoint = sc.getConf.get("spark.nabu.fireshots_url")

  val CredentialResult = Credential.getCredentialData(CredentialPayload(s"$token", cred_id, cred_type, s"$endpoint"))

  val ldap = CredentialResult.data match {
    case ldap: ldap => ldap
    case _ => throw new Exception("Currently unable available for other credentials Types")
  }

  val user = ldap.username
  val pass = ldap.password

  println(s"User: $user, Password: $pass")

} match {
  case Success(_) => println("completed successfully")
  case Failure(f) => {
    println(f)
    throw f
  }
}
