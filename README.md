# Personally Identifiable Information (PII) Anonymization

## Preface
Personally identifiable information (PII), or sensitive personal information (SPI), is information that can be used on its own or with other information to identify, contact, or locate a single person, or to identify an individual in context.

Data anonymization is a type of information sanitization whose intent is privacy protection. It is the process of either encrypting or removing personally identifiable information from data sets, so that the people whom the data describe remain anonymous.

The European Union's new General Data Protection Regulation demands that stored data on people in the EU undergo either an anonymization or a pseudonymization process.

## Functional Overview
This project is built to anonymize PII or other sensitive data from text. It makes use of [Apache Spark](https://spark.apache.org/releases/spark-release-2-3-3.html) and [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/), and is written in Java.

The input is a `.csv` file, and the resulting file after anonymization is saved as `result.csv`. The user also needs to provide the `columnName` that contains text to be anonymized.

## Technical Overview
Using Spark 2.3, the source file is read into a Dataset from which the anonymization column is extracted. Each row of this Dataset is passed to the anonymization function and using the Stanford CoreNLP engine, tokens are identified as `PERSON`, `LOCATION`, `ORGANIZATION`, `EMAIL`, `CITY`, `STATE_OR_PROVINCE` or `RELIGION`. The token is then replaced with `ANONYMIZED_VALUE`.

More details about Stanford CoreNLP and how tokens are classified can be found [here](https://stanfordnlp.github.io/CoreNLP/ner.html). The process used is called *Named Entity Recognition*.

The returned RDD after anonymization is used to create a Dataset, which is then joined with the original Dataset to give us the anonymized Dataset.

This Dataset is then saved to the target directory.

## Examples
*These sentences are fictional.*
### Example 1
Source text:
> James called Michael and wanted to know if he will be in London next week.

Anonymized text:
> ANONYMIZED_VALUE called ANONYMIZED_VALUE and wanted to know if he will be in ANONYMIZED_VALUE next week.

### Example 2
Source text:
> Roy is an employee at Google. By mistake, he publicly shared his email roysmith@google.com and later deleted it.

Anonymized text:
> ANONYMIZED_VALUE is an employee at ANONYMIZED_VALUE. By mistake, he publicly shared his email ANONYMIZED_VALUE and later deleted it.