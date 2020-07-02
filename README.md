# HBase Coprocessor

## Loading Coprocessors

### Static Loading

1.Define the Coprocessor in hbase-site.xml, with a <property> element with a <name> and a <value> sub-element. 
The <name> should be one of the following:

* `hbase.coprocessor.region.classes` for RegionObservers and Endpoints.
* `hbase.coprocessor.wal.classes` for WALObservers.
* `hbase.coprocessor.master.classes` for MasterObservers.

Value tag must contain the fully-qualified class name of your coprocessor’s implementation class.

For example to load a Coprocessor (implemented in class SumEndPoint.java) you have to create following entry in 
RegionServer’s 'hbase-site.xml' file (generally located under 'conf' directory):

><property>
    <name>hbase.coprocessor.region.classes</name>
    <value>org.myname.hbase.coprocessor.endpoint.SumEndPoint</value>
</property>

2.Put your code on HBase’s classpath. One easy way to do this is to drop the jar (containing you code and all the
 dependencies) into the lib/ directory in the HBase installation.

3.Restart HBase.

### Dynamic Loading

You can also load a coprocessor dynamically, without restarting HBase. This may seem preferable to static loading, 
but dynamically loaded coprocessors are loaded on a per-table basis, and are only available to the table for which 
they were loaded. For this reason, dynamically loaded tables are sometimes called Table Coprocessor.

In addition, dynamically loading a coprocessor acts as a schema change on the table, and **the table must be taken offline** 
to load the coprocessor. Otherwise may cause RIT (Regions in Transition)

---

**EndPointCoprocessor prefer static loading than dynamic loading.**