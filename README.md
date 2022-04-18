# To run the parser
First, change the db path in adil-parser/config.properties to your test.db path. <br /> 
Second, change the URL, userName, password in /adil-parser/src/main/java/edu/sdsc/utils/SQLParseUtil.java to your database settings.
Then, run
<pre><code>cd src/main/java/edu/sdsc/adil
jjtree Adil.jjt
javacc Adil.jj</code></pre>
Set adil-parser/src/main/java/edu/sdsc/awesome/adil/parser/AdilFirstPass.java as main class, pass
<pre><code>-i path-to-the-adil-script-path </code></pre>
to the command line arguments. Run.

# Create toy table
To parse the script adil-parser/testScripts/databaseTest.adil, create a table called ``test`` in your database with columns ``name`` (varchar) and ``age`` (integer). <br /> 
To parse the script adil-parser/entityNetworkAnalysis.adil, create a table called newsdatatable with column  ``newsID``(integer), ``newsText``(varchar), ``newspaperName``(varchar), ``newsDate``(date).


# Creat jar 
mvn clean package

mvn spring-boot:run
