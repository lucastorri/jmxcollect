package co.torri

import java.io._
import javax.management.remote._
import java.util._
import javax.management._
import javax.management.openmbean._
import java.util.concurrent.Semaphore
import scala.collection.mutable.ListBuffer


package object jmxmonitor {

    trait Writer {
        def write(epoch: Long, attr: JMXAttribute, value: Any)
    }

    class Config {
        val rds = ListBuffer[JMXAttributeRequestDefinition]()

        def add(rd: JMXAttributeRequestDefinition) = rds += rd
        def interval = 1
        def writer = null

        override def toString = rds.size.toString
    }
    object Config {
        def apply() = new Config
    }

    case class JMXServer(connector: JMXConnector, c: Config) {
        def get(attr: JMXAttribute) : JMXAttributeRequestDefinition = {
            val rd = JMXAttributeRequestDefinition(attr=attr, e=c.interval, s=this, wt=c.writer)
            c.add(rd)
            rd
        }

        def close =
            connector.close
    }

    case class JMXObject(name: String) {
        def attr(attr: String) =
            JMXAttribute(this, attr)
    }

    case class JMXAttribute(obj: JMXObject, name: String)


    case class JMXAttributeRequestDefinition
        (attr: JMXAttribute, var e: Int, var wt: Writer, var m: Any => Any = {a=>a}, val s: JMXServer) {

        def every(t: Int) : JMXAttributeRequestDefinition = { e=t; this }
        def map[R](f : Any => R) : JMXAttributeRequestDefinition = { m=f ; this }
        def writeTo(w: Writer) : JMXAttributeRequestDefinition = { wt=w; this }
        def and : JMXServer = s

        def load = {
            m(s.connector.getMBeanServerConnection.getAttribute(new ObjectName(attr.obj.name), attr.name))
        }

        override def toString = "[" + attr.obj.name + ", " + attr.name + "]"
    }


    case class JMXCompositeData(d: CompositeData) {
        def extract(data: String, others: String*) =
            (data :: others.toList).map(n => (n, d.get(n))).toMap
    }

    case class JMXDataCollector(c: Config) {

        private val threads = ListBuffer[CollectThread]()
        private val semaphore = new Semaphore(1) {
            def get =
                super.reducePermits(1)
        }

        def collect(f: Config => Unit) = {
            f(c);
            threads ++= c.rds.map { rd =>
                val t = new CollectThread(rd)
                t.start
                t
            }
            this
        }

        private case class CollectThread(rd: JMXAttributeRequestDefinition) extends Thread {
            var running = true
            override def run {
                semaphore.get
                while (running) {
                    Thread.sleep(rd.e * 1000)
                    rd.wt.write(System.nanoTime, rd.attr, rd.load)
                }
                semaphore.release
            }
        }

        def stop = {
            threads.foreach(_.running = false)
            semaphore.acquire
            c.rds.foreach(_.s.close)
        }

    }

    def using(c: Config) = JMXDataCollector(c)

    def obj(on: String) : JMXObject = JMXObject(on)

    def compositeData(d : Any) : JMXCompositeData = JMXCompositeData(d.asInstanceOf[CompositeData])


    def from(host: String, port: Int)(implicit c: Config) : JMXServer = {
        val url = new JMXServiceURL("rmi", "", 0, "/jndi/rmi://" + host + ":" + port + "/jmxrmi");
        val connector = JMXConnectorFactory.newJMXConnector(url, new HashMap[String, Object])
        connector.connect
        JMXServer(connector, c)
    }
}


object main {

    def main(args: Array[String]) {
        import loadtest._

        val stdout = new AnyRef with Writer {
            def write(epoch: Long, attr: JMXAttribute, value: Any) = println(epoch, attr, value)
        }

        val collector = using(Config()).collect { implicit config =>

            from("localhost", 1234)
                .get(obj("java.lang:type=Memory").attr("HeapMemoryUsage"))
                .every(1 /*second*/)
                .map(compositeData(_).extract("used", "max"))
                .writeTo(stdout)
                .and
                .get(obj("java.lang:type=Memory").attr("NonHeapMemoryUsage"))
                .every(5 /*second*/)
                .map(compositeData(_).extract("used", "max"))
                .writeTo(stdout)

        }

        Thread.sleep(10000)

        collector.stop

    }

}












