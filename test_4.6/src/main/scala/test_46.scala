import scala.io.StdIn

object test_46 extends App {
  // Задача п. 3, подпункт а
  val msg_1: String = "Hello, Scala!"
  val msg_2: String = "and goodbye python!"
  val msg_3: String = msg_1.replace("!", "")

  println("Задача п. 3, подпункт а")
  println(msg_1)
  println(msg_1.reverse)
  println(msg_1.toLowerCase)
  println(msg_3)
  println(msg_3 + " " + msg_2)
  println()

  // Задача п. 3, подпункт b
  def func_b(sal:Double, pers:Double, food:Double): Double = {
    val res_b = (sal + sal * pers + food) / 12
    res_b
  }
  println("Задача п. 3, подпункт b")
  println(func_b(100, 0.05, 20))
  println()

  // Задача п. 3, подпункт c
  val salary = List(100, 150, 200, 80, 120, 75)

  def func_c(sal: Double): Double = {
    val res_c = sal / (salary.sum / salary.length) * 100 - 100
    res_c
  }
  println("Задача п. 3, подпункт c")
  println("Delta % salary list: " + salary.map(sal => func_c(sal)).mkString(", "))
  println()

  // Задача п. 3, подпункт d
  val extr = List(10, -5, 8, -10, 3, -1)
  val new_sal = (salary, extr).zipped.map(_ + _).sorted

  println("Задача п. 3, подпункт d")
  println("min salary: "+ new_sal.head + ", max salary: " + new_sal.last)
  println()

  // Задача п. 3, подпункт e
  val new_sal_2 = salary ::: List(350, 90)
  println("Задача п. 3, подпункт e")
  println("New salary list: " + new_sal_2.sorted.mkString(", "))
  println()

  // Задача п. 3, подпункт f
  val ind: Int = new_sal_2.sorted.indexWhere(element => element > 130)
  println("Задача п. 3, подпункт f")
  println("New salary list: " + (new_sal_2.sorted.take(ind) ++ List(130) ++ new_sal_2.sorted.drop(ind)).mkString(", "))
  println()

  // Задача п. 3, подпункт f
  val shr_min: Int = StdIn.readLine("Введите нижнюю границу: ").toInt
  val shr_max: Int = StdIn.readLine("Введите верхнюю границу: ").toInt
  var i: Int = 0
  val lst = new_sal_2.sorted

  // Задача п. 3, подпункт g
  println("Задача п. 3, подпункт g")
  print("Salary list members: ")

  while (i < new_sal_2.length) {
    if (lst(i) > shr_min && lst(i) < shr_max)  print(i + " ")
    i += 1
  }
  println()
  println()

  // Задача п. 3, подпункт h
  println("Задача п. 3, подпункт g")
  val index: Double = StdIn.readLine("Введите процент увеличения: ").toDouble

  def func_h(sal: Double): Double = {
    val res_h = sal * (1 + index)
    res_h
  }
  println("Changed salary list: " + new_sal_2.map(sal => func_h(sal)).mkString(", "))

}




