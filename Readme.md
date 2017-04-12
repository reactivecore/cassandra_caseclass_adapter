Cassandra Case Class Adapters
=============================

Maps Case classes to the Cassandra Java Driver using shapeless powered type classes.

Note: 

* this is in early development stage
* meant as a case study so far
* there are no public builds yet
* contains weird hacks and class casts
* not API stable

Thanks a lot to the [Shapeless](https://github.com/milessabin/shapeless) team for their wonderful work.

Usage
-----

Define a case class

    case class Person (
        id: UUID,
        name: String,
        age: Int
      )
      
Have a cassandra table
      
      
      CREATE TABLE persons (
        id UUID,
        name TEXT,
        age INT,
        PRIMARY KEY (id)
      );
      

And let the Adapter automatically generated
      
      import package net.reactivecore.cca.CassandraCaseClassAdapter

      val adapter = CassandraCaseClassAdapter.make[Person]("persons")
      val session = // open cassandra session
      val person = Person(UUIDs.random(), "John Doe", 42)
      adapter.insert(person, session)
      
      val back: Seq[Person] = adapter.loadAllFromCassandra(session)
       
Supported Types
---------------
* Most fundamental types
* Option (flaky)
* Set
* Primitive UDT handling


Not supported / tested yet
--------------------------
* List
* Map
* Combination of Set or UDT with Option
* Adding custom types


Testing
-------
* The unit tests need a Cassandra Instance running on `127.0.0.1`
* They will recreate a keyspace called `unittest` on each single testcase.