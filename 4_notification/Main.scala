// import javax.management.Notification
// Define two case classes as below:
// abstract class Notification
// case class Email(sender: String, title: String, body: String) extends Notification
// case class SMS(caller: String, message: String) extends Notification
// Define a function showNotification which takes as a parameter the abstract type Notification and matches on the
// type of Notification (i.e. it figures out whether it’s an Email or SMS).
// In the case it’s an Email(email, title, _) return the string: s"You got an email from $email with title: $title“
// In the case it’s an SMS return the String: s"You got an SMS from $number! Message: $message“
abstract class Notification

case class Email(sender:String, title:String, body:String) extends Notification
case class SMS(caller:String, message:String) extends Notification

def showNotification(media : Notification) = {
    media match {
        case Email(sender, title, body) => {
            s"you got a mail from $sender with title as $title"
        }
        case SMS(caller, message) => {
            s"you got a sms from $caller with message as - $message"
        }
    }

}
object Main {
    def main(args : Array[String]) : Unit = {

        val emailNotification = Email("sender@example.com", "Hello", "How are you?")
        val smsNotification = SMS("1234567890", "Are you there?")

        println(showNotification(emailNotification)) 
        println(showNotification(smsNotification))
    }
}