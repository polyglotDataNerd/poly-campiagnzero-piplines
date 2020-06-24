# PDF to Text Parser

The parser has been specifically created to take PDF files and optical character recognition to convert to flat text files. 

**Application Arguments**

| Argument        | Sample           | Required  |
| ------------- |:-------------:| -----:|
| Source Directory | AWS s3 path for PDF files | Yes  |
| Target Directory | landing s3 directory of data i.e. s3://campaign-zero/excel/csv/| Yes  |


Summary
-        

The impetus of building this package was to be able to collect and convert all PDF formatted documents into a flat raw object to collect history and make compatible for database ingestion. This is especially useful when doing NLP modeling on certain language charatersitics to be able to classify and categorize.

In this particular use case the example used is a police contract coming from the Brentwood Police Deparment in [PDF Format](../images/Brentwood_CA.pdf) the parser then takes optical character recognition technology to convert to a [Raw Text Format](../images/Brentwood_CA.csv). This package will be able to collect versions of it in the background for historical context and to be able to inserted into a database.

**Details**
  
**[Package Location](../src/main/scala/com/sg/transformers/ocr)**

 - [**OCR Parser Class**](../src/main/scala/com/sg/transformers/ocr/OCRParserPDF.scala): Pipeline class that does the PDF conversion specifically this function [pdfDFText](../src/main/scala/com/sg/transformers/ocr/OCRParserPDF.scala) to collect history and make compatible for database ingestion. 


