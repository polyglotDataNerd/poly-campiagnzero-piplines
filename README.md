# Campaign Zero Pipeline Suite

This Github repo houses a collection package of ETL/ELT pipeline functions to enable Campaign Zero to automate their data collection process from a plethora of different and disparate data sources. The hope is to enable data minded people to use an insightful and socially aware driven platform using public/private data in a data driven manner.  


Summary
- 

[Campaign Zero](https://www.joincampaignzero.org/) is a non-profit organization that is informed by data, research and human rights principles to drive policy specifically the way police serve our communities.  While this is a high-level synopsis of what there mission statement is, please click on the link above to get full context of the great work they are doing. 

**Requirements and Dependencies**

A list of requirements to successfully run these pipelines. 
  
1. Domain and Infrastructure: These pipelines are specifically written for the AWS Cloud Domain. The main collection data store where all the raw disparate data sources will live in [AWS S3](https://aws.amazon.com/products/storage/data-lake-storage/).

2. Application Dependencies:
    * [AWS S3](https://aws.amazon.com/s3/): The data store
    * [Apache Spark 2.4.5](https://spark.apache.org/): The processing framework
    * [Scala 2.11](https://www.scala-lang.org/download/2.11.10.html): The language used to write the pipelines
    * [AWS EMR](https://aws.amazon.com/emr/): Managed Big Data Service

3. Build Tool: This projects uses maven as the build tool, you need to install a local repo on your machine in order
               to generate dependant libraries within the pom.xml file to be able to compile the project. 
               
        Follow this tutorial to setup quickly:
        install: 
         1. manual: https://maven.apache.org/install.html
         2. homebrew (preferred): http://brewformulas.org/Maven
        quick guide: https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html

    * This is a java based framework, you will need to have a [JDK](https://openjdk.java.net/install/) installed to properly compile this project. 

4. [Application Entry Point](./src/main/scala/com/sg/transformers/OCREntry.scala): This link will direct to the entry point of the application. All arguments will pass through here to be able to run the individual pipelines.

5. [Click to read all application Read.me's](./docs)
