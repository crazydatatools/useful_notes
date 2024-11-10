# Evaluation Rules
- Call by value: evaluates the function arguments before calling the function
- Call by name: evaluates the function first, and then evaluates the arguments if need be
def example = 2      // evaluated when called
val example = 2      // evaluated immediately
lazy val example = 2 // evaluated once when needed
def square(x: Double)    // call by value
def square(x: => Double) // call by name

# Higher order functions
These are functions that take a function as a parameter or return functions.
// Called like this
sum((x: Int) => x * x * x)          // Anonymous function, i.e. does not have a name
sum(x => x * x * x)                 // Same anonymous function with type inferred
// sum takes a function that takes an integer and returns an integer then
// returns a function that takes two integers and returns an integer
def sum(f: Int => Int): (Int, Int) => Int =
  def sumf(a: Int, b: Int): Int = f(a) + f(b)
  sumf

Traits are similar to Java interfaces, except they can have non-abstract members:
trait Planar:
  ...
class Square extends Shape with Planar
Classes and objects are organized in packages (package myPackage).

They can be referenced through import statements (import myPackage.MyClass, import myPackage.*,
object Hello extends App:
  println("Hello World")