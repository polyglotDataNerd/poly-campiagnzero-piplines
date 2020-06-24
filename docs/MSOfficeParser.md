# Excel Parser

The parser has been specifically created to act with the Microsoft Office suite specifically Excel.

**Application Arguments**

| Argument        | Sample           | Required  |
| ------------- |:-------------:| -----:|
| Source Directory | AWS s3 path of file/files | Yes  |
| Target Directory | landing s3 directory of data i.e. s3://campaign-zero/excel/csv/| Yes  |


Summary
-        

The impetus of building this package was to be able to collect and convert all Excel formatted documents into a flat raw object to collect history and make compatible for database ingestion. 

In this particular use case the example was the site [Mapping Police Data](https://mappingpoliceviolence.org/) and the downloadable [dataset](https://mappingpoliceviolence.org/s/MPVDatasetDownload.xlsx). This package will be able to collect versions of it in the background for historical context and to be able to inserted into a database.

**Details**

This package relies on a utility function that requests data from an API and puts into a directory in AWS s3. 

[**Spark Utility Class**](../src/main/scala/com/sg/transformers/utility/SparkUtils.scala): This class holds helper functions that is used all over the package.

 - [Get API Function](../src/main/scala/com/sg/transformers/utility/SparkUtils.scala#L145): This function will call an HTTP GET request and collect the data into AWS s3.
 
 **Application Arguments**
 
 | Argument        | Sample           | Required  |
 | ------------- |:-------------:| -----:|
 | Source Bucket | AWS s3 bucket | Yes  |
 | Source Key | path and name of data to be collected i.e. s3://campaign-zero/excel/csv/output_example.example| Yes  |
 | Source URL | link of HTTP Path i.e. https://mappingpoliceviolence.org/s/MPVDatasetDownload.xlsx | Yes  |

 This function works on all HTTP GET requests and can be reused in other HTTP driven pipelines using the arguments above.
  
**[Package Location](../src/main/scala/com/sg/transformers/ocr)**

 - [**Microsoft Parser Class**](../src/main/scala/com/sg/transformers/ocr/OCRParserMS.scala): Pipeline class that pulls Excel sheets into flat raw objects to collect history and make compatible for database ingestion. 


