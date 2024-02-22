# Week-05 Lab

## Objectives

- Demonstrate the knowledge of constructing end-to-end DataFrame and Dataset Spark applications
- Demonstrate working code and knowledge of statistics
- Understand and utilize Pyspark and Spark Scala API Documentation

## Assignment Setup

- Using the file `sf-fire-calls.csv`, file write a Pyspark application that answers the seven questions on Page 68 of the Text Book under the header **End-to-End DataFrame Examples**
  - **Note** do not use the notebook file provided, we want to challenge you to be able to build this yourself

## Assignment Details - Part I

- Create a Pyspark application named: `assignment_02.py`
  - In that application code add seven comments (# pound sign) that type out the question from the text book
  - The schema is provided in the textbook on the preceding pages
  - Provide code to answer the questions below each header
  - You can provide a single "read" of the source code at the top of the file into a DataFrame -- each question does not require a `read()`
  - Unless noted for the output you can use a `.show(10)` to truncate the output 
  - Run the source code via Spark-Submit on your Vagrant Box
  - Once you have the answer to the question, go back and add a source code comment with the answer under the question
  - Sample code is available in the book LearningSparkV2 sample code
    - `~/LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire-calls.csv`
  - Like the MnMcount code -- take the dataset input as a commandline variable
    - Do not hardcode the dataset
  - Make sure to commit and push code to GitHub continually. Assignments that have the entire code submitted with none or little commit history will not be accepted
    - Commit and push often.
  - Don't share the answers with others! Your work is individual
  - [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html "webpage for Pyspark API")


## Assignment Details - Part II

- Create a Spark Scala application named `assignment_02.scala`
  - Create the necessary sbt build infrastructure so your code can be compiled
  - The data is available under the LearningSparkV2 book example code
    - LearningSparkV2 > databricks-datasets > learning-spark-v2 > iot-devices > iot_devices.json
  - This is a json file, schema is provided in the textbook on the preceding pages
  - Create the proper `build.sbt` and additional src scaffolding
  - Answer the 4 questions on page 74 under the header: **End-to_end Dataset example**
  - In your source code add a comment stating the question and provide the code below to answer the question
  - Compile your jar file via the `sbt` command
  - Run the jar file via the `spark-submit` command
  - Once you have the answers, add it via a comment back to your source code.
  - Make sure to commit and push code to GitHub continually.  Assignments that have the entire code submitted with none or little commit history will not be accepted.  Commit and push often.
  - Don't share the answers with others.  Your work is individual
  - [Scala API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html "Scala API documentation")

### Deliverable

Create a sub-folder under `labs` named: `week-05` under the `itmd-521` folder. Place all deliverables there.  But not the dataset.

Submit to Blackboard the URL to the folder in your GitHub repo. I will clone your code and run it to test the functionality. I don't need the datasets as I will have them already.

Due at the **Start of class** Section 05 February 14th 1:50 PM CST
Due at the **Start of class** Section 01,02,03 February 15th 3:05 PM CST

Vamsi
![python Q1](1.png)
![Python Q2](2.png)
![python Q3](3.png)
![python Q4](4.png)
![python Q5](5.png)
![pyhton Q6](6.png)
![python Q7](7.png)
![python code](<py 1.png>)
![python code](<py 2.png>)
![scala code](<scala code.png>)
![scala run](<scala run.png>)