import scala.collection.mutable.Map

// Define a case class for an item with name and quantity
case class Item(name: String, quantity: Int)

object ItemsOperations {
  def main(args: Array[String]): Unit = {
    // Initialize the collection of items
    var items = Map(
      "Butter" -> 20,
      "Bun" -> 10,
      "Egg" -> 7,
      "Biscuit" -> 25,
      "Bread" -> 15
    )

    // i. Display item-name and quantity
    println("i. Item-name and quantity:")
    items.foreach { case (name, quantity) =>
      println(s"$name: $quantity")
    }

    // ii. Display sum of quantity and total number of items
    val totalQuantity = items.values.sum
    val totalItems = items.size
    println(s"\nii. Sum of quantities: $totalQuantity")
    println(s"Total number of items: $totalItems")

    // iii. Add 3 Buns to the collection
    val additionalBuns = 3
    items("Bun") += additionalBuns

    // iv. Add new item "Cheese" with quantity 12 to the collection
    val newItemName = "Cheese"
    val newItemQuantity = 12
    items += (newItemName -> newItemQuantity)

    // Display the updated collection after modifications
    println("\nUpdated item-name and quantity:")
    items.foreach { case (name, quantity) =>
      println(s"$name: $quantity")
    }
  }
}
