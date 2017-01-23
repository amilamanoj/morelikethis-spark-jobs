# morelikethis-spark-jobs
MoreLikeThis Spark Jobs with Word2Vec

##Configuration Settings
-----
All setting related to SocioCortext, Word2Vec and Scheduling should be added to a conf file ex. here my.conf
Note: This uses under lying mechanism of TypeSafe Config library of Scala

Configuration File Entries can be of format as below
```
sc {
    api {
        baseurl: "...",
        version: "..."
    }
}
```

Loading Configuration from a file is easy.
Place your conf file in src/resources, then add the below code to wherever you want to load
```
val config = ConfigFactory.load("my.conf")
```

Retrieving the configuration
```
val configString = config.getString("....")
```
or if you want to retrieve it as an object then
```
val configObject [ config.getObject(".....")