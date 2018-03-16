package edu.knoldus

//import org.apache.log4j.Logger
import com.datastax.driver.core.{ConsistencyLevel, ResultSet, Session}

import scala.collection.JavaConverters._

object CassandraApplication extends App with CassandraSession {
 // val log = Logger.getLogger(getClass)
 cassandraSession.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)
 println("\n\n*********Cluster Information *************")
 println("\n\n Cluster Name is: " +
  cassandraSession.getCluster.getClusterName)
 println("\n\n Cluster Configuration is: " +
  cassandraSession.getCluster.getConfiguration.getQueryOptions.getConsistencyLevel)
 println("\n\n Cluster Metadata is: " +
  cassandraSession.getCluster.getMetadata.getAllHosts.toString)

 private def tableCreation(sessionCreate: Session): Unit = {
  println()
  println()
  println("*******table creation and fetching all values*********")
  println()
  // sessionCreate.execute(s"create table if not exists employee(emp_id int primary key,emp_city text,emp_name text,emp_phone varint,emp_salary varint)")
  val results = sessionCreate.execute(s"create table if not exists employeesdata2(emp_id int," +
   s"emp_city text,emp_name text,emp_phone varint,emp_salary varint," +
   s"primary key(emp_id,emp_salary))")
  println("table created successfully>>>>>>>>>>>>>>>>>>")
  println(s"consistency level set to quorum>>>>>>>>>>>>>>")

  //  val results = sessionCreate.execute(s"select * from employee").asScala.toList
 }

 private def recordsInsertion(sessionCreated: Session): Unit = {
  println()
  println()
  println("*******now inserting records in table emp*********")
  val firstRecord = sessionCreated.execute(s"insert into employeesdata2(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(1,'chandigarh','raja',78005987,40000)")
  val secondRecord = sessionCreated.execute(s"insert into employeesdata2(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(2,'kanpur','shubham',6788405987,25000)")
  val thirdRecord = sessionCreated.execute(s"insert into employeesdata2(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(3,'chandigarh','raju',7989305987,45000)")
  val fourthRecord = sessionCreated.execute(s"insert into employeesdata2(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(4,'pakistan','nitin',8975875987,16000)")
  val resultSet = sessionCreated.execute(s"select * from employeesdata2")
  resultSet.forEach(println(_))
  println("records inserted>>>>>>>>>>>>>>>>")
 }

 private def fetchThroughEmployeeId(sessionCreate: Session): Unit = {
  println()
  println()
  println("*******fetching through particular employee id********")
  val results = sessionCreate.execute(s"select * from employeesdata2 where emp_id = 2").asScala.toList
  results.foreach(println(_))
 }

 private def updateFourthRecord(sessionCreate: Session): Unit = {
  println()
  println()
  println("********fourth row updated*********")
  val results = sessionCreate.execute(s"update employeesdata2 set  emp_city = 'chandigarh' where emp_id = 4 and emp_salary = 16000 ")
  val updatedResult = sessionCreate.execute(s"select * from employeesdata2")
  updatedResult.forEach(println(_))
 }

 private def salaryRecords(sessionCreated: Session): Unit = {
  println()
  println()
  println("salary greater than 30000")
  //    val query = "create index on employee(emp_salary)"
  //    val result = cassandraSession.execute(s"select emp_id,emp_city,emp_name,emp_phone,min(emp_salary) as salary from employee group by" +
  //     s"emp_id,emp_city,emp_name,emp_phone having min(emp_salary)>20000")
  val results = cassandraSession.execute(s"select * from employeesdata2 where emp_id = 1 and emp_salary > 30000")
  results.forEach(println(_))
 }

 private def chandigarhDetails(sessionCreate: Session): Unit = {
  println()
  println()
  println("all chandigarh residents>>>>>>>>>>>>>")
  //sessionCreate.execute(s"create index on employeesdata2(emp_city)")
  val result = sessionCreate.execute(s"select * from employeesdata2 where emp_city = 'chandigarh'")
  result.forEach(println(_))
 }

 private def deleteChandigarhDetails(sessionCreated: Session): Unit = {
  println()
  println()
  println("chandigarh records deleted>>>>>>>>>>>>>>>")

  sessionCreated.execute(s"create table if not exists deletechandigarh(emp_id int," +
   s"emp_city text,emp_name text,emp_phone varint,emp_salary varint," +
   s"primary key(emp_city))")


  val firstRecord = sessionCreated.execute(s"insert into deletechandigarh(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(1,'chandigarh','raja',78005987,40000)")
  val secondRecord = sessionCreated.execute(s"insert into deletechandigarh(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(2,'kanpur','shubham',6788405987,25000)")
  val thirdRecord = sessionCreated.execute(s"insert into deletechandigarh(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(3,'chandigarh','raju',7989305987,45000)")
  val fourthRecord = sessionCreated.execute(s"insert into deletechandigarh(emp_id,emp_city,emp_name,emp_phone," +
   s"emp_salary)values(4,'pakistan','nitin',8975875987,16000)")

  sessionCreated.execute(s"delete from deletechandigarh where emp_city = 'chandigarh'")

  val result = sessionCreated.execute(s"select * from deletechandigarh")
  result.forEach(println(_))
 }

 tableCreation(cassandraSession)
 recordsInsertion(cassandraSession)
 fetchThroughEmployeeId(cassandraSession)
 updateFourthRecord(cassandraSession)
 salaryRecords(cassandraSession)
 chandigarhDetails(cassandraSession)
 deleteChandigarhDetails(cassandraSession)
}
