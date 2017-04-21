Cassandra Case Class Adapters
=============================

[![Build Status](https://travis-ci.org/reactivecore/cassandra_caseclass_adapter.svg?branch=master)](https://travis-ci.org/reactivecore/cassandra_caseclass_adapter)

Maps Case classes to the Cassandra Java Driver using shapeless powered type classes.

Note: 

* this is in early development stage
* contains weird hacks and class casts
* not API stable

Thanks a lot to the [Shapeless](https://github.com/milessabin/shapeless) team for their wonderful work.

Add Dependency
--------------

Builds are available for Scala 2.10, 2.11, 2.12 having a dependency to the Cassandra Driver 3.0.7.

Add this to your `build.sbt`:

    libraryDependencies += "net.reactivecore" %% "cassandra-caseclass-adapter" % "0.0.1"
    
If you use Scala 2.10, you also need (see [Shapeless Documentation](https://github.com/milessabin/shapeless#shapeless-232-with-sbt))
    
    libraryDependencies += compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

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
      
      import net.reactivecore.cca.CassandraCaseClassAdapter

      val adapter = CassandraCaseClassAdapter.make[Person]("persons")
      val session = // open cassandra session
      val person = Person(UUIDs.random(), "John Doe", 42)
      adapter.insert(person, session)
      
      val back: Seq[Person] = adapter.loadAllFromCassandra(session)

Supported
---------
* boiler plate free `fromRow`, `insert`, `loadAllFromCassandra`

Supported Types
---------------
* Most fundamental types
* Option
* Set of Primitives and of UDT
* List of Primitives (as Seq) and of UDT
* Primitive UDT handling


Not supported / tested yet
--------------------------
* Map
* Tuples
* Combination of Set/Seq with more than one UDT
* Adding custom types
* Prepared Statements for fast insert

Not planned (yet)
-----------------
* Query Helpers


Testing
-------
* The unit tests need a Cassandra Instance running on `127.0.0.1`
* They will recreate a keyspace called `unittest` on each single testcase.