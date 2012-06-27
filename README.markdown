TM3
===

"TM3" is the name of the Translation Memory storage engine added to
[GlobalSight](http://globalsight.com) as part of work for the [TAUS Data
Association](http://www.tausdata.org).  The code in this repository contains a
standalone version that can be run on its own or embedded in other tools.  The
code is adapted from the GlobalSight trunk (version 8.2).

Using TM3 in GlobalSight
--------------------------

As of GlobalSight 8.2, TM3 is not enabled by default; it can be enabled per-company by the superadmin through the settings page for each individual company.

Design
------

TM3 is built entirely on top of MySQL.  However, beyond a few static tables, most of the tables are managed dynamically by the library.  Each logical TM is represented by multiple tables that are collectively referred to as a *tablespace*.

TM3 can create 3 types of TM:
* *Bilingual* - A TM that has a dedicated tablespace but only a single source and target locale.  The fixed locales allow for slightly smaller row sizes.
* *Multilingual* - A TM that has a dedicated tablespace and can support any number of source and target locales.
* *Multilingual Shared* - A TM that can support any number of source and target locales but does not have a dedicated tablespace.  Rather, its tablespace is a *Storage Pool* shared with one or more other Multilingual Shared TMs.  Multiple Storage Pools are managed separately through the API.  For example, in GlobalSight 8.2, most TM3 TMs use Multilingual Shared, with one Storage Pool allocated for each company in GlobalSight.

Building and Testing
--------------------

GlobalSight is built with ant, but this standalone version of TM3 uses
[maven](http://maven.apache.org/).

Unittests are heavily database-based.  The 'test' phase will attempt to
use a database called `tm3_test`, which is cleaned up afterwards.  To do
this, you must configure a server called `tm3-database-credentials` in
your `settings.xml` file.

If you don't have a settings.xml already configured, create one 
(`~/.m2/settings.xml`) that looks like this:

<pre>
    &lt;settings&gt;  
      &lt;servers&gt; 
        &lt;server&gt;
          &lt;id&gt;tm3-database-credentials&lt;/id&gt;
          &lt;username&gt;(your database user)&lt;/username&gt;
          &lt;password&gt;(your database password)&lt;/password&gt;
        &lt;/server&gt; 
      &lt;/servers&gt;  
    &lt;/settings&gt;
</pre>

Use of Hibernate and SQL
------------------------
TM3 It makes some use of [Hibernate](http://hibernate.org), as that is what 
is used elsewhere in GlobalSight.  However, the bulk of TM3's functionality 
is implemented as direct SQL calls, because of Hibernate's difficulty in 
mapping entities to dynamically managed tables.

Additionally, because it was developed against an old (3.1) version of
Hibernate, it does not use `Session.doWork()` (which wasn't available).
Various TM3 APIs expect to be provided with a Hibernate `Session`; TM3 will
either use the Session directly or obtain its underlying JDBC connection via
`Session.connection()` and use that to execute direct SQL.  Depending on the
environment in which TM3 is being run, the use of `Session.connection()` may
leak the connection; the caller will need to implement appropriate logic to
clean it up.

License
-------

Like GlobalSight itself, all code is released under the Apache 2.0 license.
