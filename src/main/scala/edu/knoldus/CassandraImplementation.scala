/*package edu.knoldus

//import org.apache.log4j.Logger
import com.datastax.driver.core.{ResultSet, Session}
import scala.collection.JavaConverters._

object CassandraImplementation extends App with CassandraSession {
 // val log = Logger.getLogger(getClass)
 println("\n\n*********Cluster Information *************")
 println("\n\n Cluster Name is: " +
  cassandraSession.getCluster.getClusterName)
 println("\n\n Cluster Configuration is: " +
  cassandraSession.getCluster.getConfiguration.getQueryOptions.getConsistencyLevel)
 println("\n\n Cluster Metadata is: " +
  cassandraSession.getCluster.getMetadata.getAllHosts.toString)

 private def fetchThroughEmpid(sessionCreate: Session): Unit = {
  println()
  println()
  println("*******fetching through particular employee id********")
  val results = sessionCreate.execute(s"select * from employees1 where emp_id = 2")
  results.forEach(println(_))
 }

  private def tableCreation(sessionCreate: Session): Unit = {
  println()
  println()
  println("*******table creation and fetching all values*********")
  println()
  // sessionCreate.execute(s"create table if not exists employee(emp_id int primary key,emp_city text,emp_name text,emp_phone varint,emp_salary varint)")
  val results = sessionCreate.execute(s"create table if not exists employees1(emp_id int," +
  s"emp_city text,emp_name text,emp_phone varint,emp_salary varint," +
  s"primary key(emp_salary,emp_id))")
  println("table created successfully>>>>>>>>>>>>>>>>>>")
  println(s"consistency level set to quorum>>>>>>>>>>>>>>")

  //  val results = sessionCreate.execute(s"select * from employee").asScala.toList

 }

 private def recordsInsertion(sessionCreated: Session): Unit = {
  println()
  println()
  println("*******now inserting records in table emp*********")
  val firstRecord = sessionCreated.execute(s"insert into employees1(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(1,'chandigarh','raja',78005987,40000)")
  val secondRecord = sessionCreated.execute(s"insert into employees1(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(2,'kanpur','shubham',6788405987,25000)")
  val thirdRecord = sessionCreated.execute(s"insert into employees1(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(3,'chandigarh','raju',7989305987,45000)")
  val fourthRecord = sessionCreated.execute(s"insert into employees1(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(4,'pakistan','nitin',8975875987,16000)")
  val resultSet = sessionCreated.execute(s"select * from emp")
  resultSet.forEach(println(_))
  println("records inserted>>>>>>>>>>>>>>>>")
 }

 // private def updateFourthRecord(sessionCreate: Session): Unit = {
 //  println()
 //  println()
 //  println("********fourth row updated*********")
 //  val results = sessionCreate.execute(s"update employee set emp_salary = 54000, emp_city = 'chandigarh' where emp_id = 4 ")
 //  val updatedResult = sessionCreate.execute(s"select * from employee")
 //  updatedResult.forEach(println(_))
 // }
 //
 // private def salaryRecords(sessionCreated: Session): Unit = {
 //  val query = "create index on employee(emp_salary)"
 //  val result = cassandraSession.execute(s"select emp_id,emp_city,emp_name,emp_phone,min(emp_salary) as salary from employee group by" +
 //   s"emp_id,emp_city,emp_name,emp_phone having min(emp_salary)>20000")
 //    val results = cassandraSession.execute(s"select * from employee where emp_salary > 20000")
 //  result.forEach(println(_))
 // }
 //
 // private def chandigarhDetails(sessionCreate: Session): Unit = {
 //  println()
 //  println()
 //  sessionCreate.execute(s"create index on employee(emp_city)")
 //  val result = sessionCreate.execute(s"select * from employee where emp_city = 'chandigarh'")
 //  result.forEach(println(_))
 // }
 //
 //  private def deleteChandigarhDetails(sessionCreated: Session): Unit = {
 //   println()
 //   println()
 //   sessionCreated.execute(s"drop index if exists employee_emp_city_idx")
 //  sessionCreated.execute(s"create index withoutChandigarh on employee(emp_city)")
 //   val result = sessionCreated.execute(s"delete from employee where emp_city = 'chandigarh'")
 //   result.forEach(println(_))
 //  }
 //
 tableCreation(cassandraSession)
 recordsInsertion(cassandraSession)
 fetchThroughEmpid(cassandraSession)
 // updateFourthRecord(cassandraSession)
 // chandigarhDetails(cassandraSession)
 //  deleteChandigarhDetails(cassandraSession)
 // salaryRecords(cassandraSession)
 //
 //
}
*/