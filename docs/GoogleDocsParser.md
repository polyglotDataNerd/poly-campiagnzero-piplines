# Google Docs Parser

The parser has been specifically created to interact with google docs specifically google Sheets.

**Application Arguments**

| Argument        | Sample           | Required  |
| ------------- |:-------------:| -----:|
| Unique Sheet ID | The unique ID of the google sheet i.e. 11Ie6hCP9fw6d0T3XzzGwNxYjeM4z1Z36_Q8T2pToJH8 | Yes  |
| Outpath | landing s3 directory of data i.e. s3://campaign-zero/cde/| Yes  |


Summary
-        

The impetus of building this package was to be able to collect and convert all data in a google doc format into a flat raw object to collect history and make compatible for database ingestion. 

In this particular use case the example was [Nationwide Police Data](https://docs.google.com/spreadsheets/d/11Ie6hCP9fw6d0T3XzzGwNxYjeM4z1Z36_Q8T2pToJH8/edit#gid=1623145710). The process does not change for the google doc owner, users can still interact with docs and this package will be able to collect versions of it in the background for historical context.

**Requirements**

The API's to interact with google docs use the core GCP framework. In order to run the automated pipeline the owner would need: 

 - Register for Google Service Account: https://cloud.google.com/compute/docs/access/service-accounts
 - Credentials, the pipeline interaction uses private credentials to interact, the owner would need to generate credentials once the service account has been created: https://cloud.google.com/iam/docs/service-accounts#application_default_credentials
   * [OAuth Credentials](https://support.google.com/cloud/answer/6158849?hl=en): Specific credentials to be able to interact with Google API V4
 - Once the service account and credentials have been established they would need to live in secured private bucket in AWS s3 since the application will temporally import those credentials to authenticate the collection. 
  
**[Package Location](../src/main/scala/com/sg/transformers/ocr)**

 - [**Google Parser Class**](../src/main/scala/com/sg/transformers/ocr/GoogleParser.scala): Pipeline class that pulls google sheet into target directory. 

