

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import java.net.URISyntaxException
import java.util


  object Exercise0Solved {
    @throws[Exception]
    def main(args: Array[String]): Unit = {

        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
        //log(cluster.getMetadata())
        //val session = cluster.connect()
        //cluster = Cluster.builder.addContactPoints("127.0.0.1").withPort("9042").build
        //val session = cluster.
        System.out.println("*********************************************")
        System.out.println("              Exercise 0                     ")
        System.out.println("*********************************************")
        // solución del ejercicio


    }
  }