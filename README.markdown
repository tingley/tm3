TM3
===

"TM3" is the name of the Translation Memory storage engine added to
[GlobalSight](http://globalsight.com) as part of work for the [TAUS Data
Association](http://www.tausdata.org).  The code in this repository contains a
standalone version that can be run on its own or embedded in other tools.  The
code is adapted from the GlobalSight trunk (version 8.2).

The code has been deployed in production on individual translation memories of
over 100 million words apiece.

Technical Stuff
===============

Data Storage
------------

TM3 is built entirely on top of MySQL.  However, beyond a few static tables, most of the tables are managed dynamically by the library.  Each logical TM is represented by multiple tables that are collectively referred to as a *tablespace*.

TM3 can create 3 types of TM:
* *Bilingual* - A TM that has a dedicated tablespace but only a single source and target locale.  The fixed locales allow for slightly smaller row sizes.
* *Multilingual* - A TM that has a dedicated tablespace and can support any number of source and target locales.
* *Multilingual Shared* - A TM that can support any number of source and target locales but does not have a dedicated tablespace.  Rather, its tablespace is a *Storage Pool* shared with one or more other Multilingual Shared TMs.  Multiple Storage Pools are managed separately through the API.  For example, in GlobalSight 8.2, most TM3 TMs use Multilingual Shared, with one Storage Pool allocated for each company in GlobalSight.

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

Embedding
=========

A tool that uses TM3 needs to implement several interfaces:
* `TM3Data` - implementation of segment/translation unit data
* `TM3Locale` - some representation of a source/target locale
* `TM3FuzzyMatchScorer` - a class capable of producing evaluating fuzzy matches
* `TM3DataFactory` - factory class to provide implementations of the other 
   components.

`TM3Data`
--------

Different tools have different requirements for what segment data must be
stored in a TM; TM3 supports this by hiding the segment implementation behind
the `TM3Data` interface.

A `TM3Data` object must be representable as a String in order to be represented
as a text field in the database.  (Arguably, the schema should store BLOBs
instead.)  The text representation may simply be the segment content, or it may
be a more complex representation (eg, XML or JSON) with additional metadata.
Additionally, the `TM3Data` implementation is responsible for computing its own
fingerprint for performing exact matches, and producing a set of tokenized
fingerprints for fuzzy match indexing and retrieval.

`TM3Locale`
-----------

A representation of a source / target locale.  These may be backed by a
database table, but do not have to be.  (For example, the unittests use a
hardcoded set of locales that is represented entirely in memory.)

`TM3FuzzyMatchScorer`
---------------------

This interface serves as an extension point that allows for different fuzzy match scoring algorithms to be inserted into the leverage engine.

A default implementation of a modified Damerau-Levenshtein edit distance
calculation can be found in `com.globalsight.ling.tm3.core.EditDistanceScorer`.
This implementation deviates from the standard algorithm in that it is computed
by word rather than by character; as a result, the penalties applied may vary
based on the differences between the words themselves.  To use
`EditDistanceScorer`, the `TM3Data` implementation must also implement the
`TM3Scorable` interface.

`TM3DataFactory`
---------------

The `TM3DataFactory` class is responsible for producing instances of the other
support classes.  Additionally, the `extendConfiguration()` hook allows the
implementation to extend the injected Hibernate configuration with any required
entity mappings.

Exact and Fuzzy Matching
------------------------

Exact matching is performed by comparison of fingerprints.  Each `TM3Data`
implementation is responsible for producing its own fingerprints.

Fuzzy matching is performed by a 2-step process: a coarse search to identify
fuzzy match candidates, followed by a more careful scoring of each candidate.
Coarse searching is performed via wordwise trigram search, while the scoring is performed by the specified `TM3FuzzyMatchScorer` implementation.

Attributes and TU Identity
--------------------------

Each source TU can support arbitrarily many attributes.  Attributes come in two flavors:
* *Built-in attributes* are stored inline in the TU row data.  They are
  declared when the TM is created and can't be changed, other than by manually
  running a bunch of ALTER statements against the database.  However, they
  offer better performance and some basic data typing.
* *Ad-hoc attributes* are stored in a dedicated table (per-tablespace).  They
  can be added or removed at any time.  However, their values must always be
  Strings, and they are slightly slower.

Each attribute has a flag called `affectsIdentity`.  If a TM has one or more
identify-affecting attributes, then the same source TUV may exist multiple
times in the TM, as long as at least one of the identify-affecting attributes
differs in value.

Issues
======

Write Performance
-----------------

TM3 is optimized for reading, and writing is slower than I'd like.  This is
mostly because of the sheer number of rows written to generate the fuzzy
indexes (1 per trigram, which is equivalent to 1 per source word).  It is also
suspected (althought not proven) that the distribution of trigram hashes on
which the fuzzy indexes are built causes poor performance for generation of
clustered MySQL table indexes.  However, the use of clustered indexes is
critical to fuzzy lookup performance.

Lastly, exact match lookup time has been identified as a signficant (25%)
factor in write performance.  The write code performs an exact match lookup
individually on each candidate segment; this code could probably be optimized.

Concurrent writes
-----------------

TM3 uses "SELECT...FOR UPDATE" during write operations in order to prevent
duplicate source TUs from being written out.  However, it also uses Hibernate's
`Session.lock()` method to lock any Hibernate-managed objects.  None of this is
really that great.

SQL Query Generation
--------------------

Despite some basic efforts to clean add some query building logic, this
is nonetheless pretty much a mess.  It's worth investigating whether 
something like [JDBI](http://www.jdbi.org/) could help bring order to chaos
here.

Building and Testing
====================

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

Using TM3 in GlobalSight
==========================

As of GlobalSight 8.2, TM3 is not enabled by default; it can be enabled per-company by the superadmin through the settings page for each individual company.


License
=======

Like GlobalSight itself, all code is released under the Apache 2.0 license.
