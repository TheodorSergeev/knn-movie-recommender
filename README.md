# Milestone Description

[To Be Released](./Milestone-1.pdf)

Note: Section 'Updates' lists the updates since the original release of the Milestone..

# Dependencies

````
    sbt >= 1.4.7
    openjdk@8
````

Should be available by default on ````iccluster028.iccluster.epfl.ch````. Otherwise, refer to each project installation instructions. Prefer working locally on your own machine, you will have less interference in your measurements from other students.

If you have multiple installations of openjdk, you need to specify the one to use as JAVA_HOME, e.g. on OSX with
openjdk@8 installed through Homebrew, you would do:
````
    export JAVA_HOME="/usr/local/Cellar/openjdk@8/1.8.0+282";
````

# Dataset

Download [data.zip](https://gitlab.epfl.ch/sacs/cs-449-sds-public/project/dataset/-/raw/main/data.zip).

Unzip:
````
> unzip data.zip
````

It should unzip into ````data/```` by default. If not, manually move ````ml-100k```` and ````ml-25m```` into ````data/````.


# Personal Ratings

Additional personal ratings are provided in the 'data/personal.csv' file in a
csv format with ````<movie>, <movie title>, <rating>```` to test your recommender.
You can copy this file and change the ratings, with values [1,5] to obtain
references more to your liking!

Entries with no rating are in the following format:
````
1,Toy Story (1995),
````

Entries with ratings are in the following format:
````
1,Toy Story (1995),5
````

# Repository Structure

````src/main/scala/shared/predictions.scala````:
All the functionalities of your code for all questions should be defined there.
This code should then be used in the following applications and tests.

## Applications

````src/main/scala/predict/Baseline.scala````: Output answers to questions **B.X**.
````src/main/scala/distributed/DistributedBaseline.scala````: Output answers to questions **D.X**.
````src/main/scala/predict/Personalized.scala````: Output answers to questions questions **P.X**.
````src/main/scala/predict/kNN.scala````: Output answers to questions questions **N.X**.
````src/main/scala/recommend/Recommender.scala````: Output answers to questions questions **N.X**.

Applications are separate from tests to make it easier to test with different
inputs and permit outputting your answers and timings in JSON format for easier
grading.

## Unit Tests

Corresponding unit tests for each application:

````
    src/test/scala/predict/BaselineTests.scala
    src/test/scala/distributed/DistributedBaselineTests.scala
    src/test/scala/predict/PersonalizedTests.scala
    src/test/scala/predict/kNNTests.scala
    src/test/scala/recommend/RecommenderTests.scala
````

Your tests should demonstrate how to call your code to obtain the answers of
the applications, and should make exactly the same calls as for the
applications above. This structure intentionally encourages you to put as
little as possible functionality in the application. This also gives the TA a
clear and regular structure to check its correctness.

# Usage

## Execute unit tests

````sbt "testOnly test.AllTests"````

You should fill all tests and ensure they all succeed prior to submission.

## Run applications 

### Baseline

On ````ml-100k````:
````
    sbt "runMain predict.Baseline --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json baseline-100k.json"
````

On ````ml-25m````:
````
    sbt "runMain predict.Baseline --train data/ml-25m/r2.train --test data/ml-25m/r2.test --json baseline-25m.json"
````

### Distributed Baseline

````
    sbt "runMain distributed.DistributedBaseline --train data/ml-25m/r2.train --test data/ml-25m/r2.test  --separator , --json distributed-25m-4.json --master local[4]"
````

You can vary the number of executors used locally by using ````local[X]```` with X being an integer representing the number of cores you want to use locally.

### Personalized

````
    sbt "runMain predict.Personalized --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json personalized-100k.json"
````

### kNN

````
    sbt "runMain predict.kNN --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json knn-100k.json"
````

### Recommender

````
    sbt "runMain recommend.Recommender --data data/ml-100k/u.data --personal data/personal.csv --json recommender-100k.json"
````

## Time applications

For all the previous applications, you can set the number of measurements for timings by adding the following option ````--num_measurements X```` where X is an integer. The default value is ````0````.

## IC Cluster

Test your application locally as much as possible and only test on the iccluster
once everything works, to keep the cluster and the driver node maximally available
for other students.

### Assemble Application for Spark Submit

````sbt clean````: clean up temporary files and previous assembly packages.

````sbt assembly````: create a new jar
````target/scala-2.11/m1_yourid-assembly-1.0.jar```` that can be used with
````spark-submit````.

Prefer packaging your application locally and upload the tar archive of your application
before running on cluster.

### Upload jar on Cluster 

````
    scp target/scala-2.11/m1_yourid-assembly-1.0.jar <username>@iccluster028.iccluster.epfl.ch:~
````

### Run on Cluster

````
spark-submit --class distributed.DistributedBaseline --master yarn --num-executors 1 target/scala-2.11/m1_yourid-assembly-1.0.jar  --train TRAIN --test TEST --separator , --json distributed-25m-1.json --num_measurements 1
````

See [config.sh](./config.sh) for HDFS paths to pre-uploaded TRAIN and TEST datasets. You can vary the number of executors with ````--num-executors X````, and number of measurements with ````--num_measurements Y````.

## Grading scripts

We will use the following scripts to grade your submission:

    1. ````./test.sh````: Run all unit tests.
    2. ````./run.sh````: Run all applications without timing measurements.
    3. ````./timeTrials.sh````: Time applications to determine which student implementations are fastest.
    4. ````./timeOthers.sh````: Time applications to check report answers against independent measurements. 
    4. ````./timeCluster.sh````: Package and time applications on Spark Cluster.

All scripts will produce execution logs in the ````logs````
directory, including answers produced in the JSON format. Logs directories are
in the format ````logs/<scriptname>-<datetime>-<machine>/```` and include at
least an execution log ````log.txt```` as well as possible JSON outputs from
applications. 

Ensure all scripts run correctly locally before submitting. Avoid running
````timeCluster.sh```` on iccluster as the packaging and measurements will
interfere with other students working on their Milestone at the same time. If
````timeCluster.sh```` correctly runs locally on your machine, this should be
sufficient.


## Package for submission

Steps:

    1. Update the ````name````, ````maintainer```` fields of ````build.sbt````, with the correct Milestone number, your ID, and your email.
    2. Ensure you only used the dependencies listed in ````build.sbt```` in this template, and did not add any other.
    3. Remove ````project/project````, ````project/target````, and ````target/````.  
    4. Test that all previous commands for generating statistics, predictions, and recommendations correctly produce a JSON file (after downloading/reinstalling dependencies).
    5. Remove the ml-100k dataset (````data/ml-100k.zip````, and ````data/ml-100k````), as well as the````project/project````, ````project/target````, and ````target/````. 
    6. Remove the ````.git```` repository information.
    7. Add your report and any other necessary files listed in the Milestone description (see ````Deliverables````).
    8. Zip the archive.
    9. Submit to the TA for grading.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

Explore Spark Interactively (supports autocompletion with tabs!): https://spark.apache.org/docs/latest/quick-start.html

Scallop Argument Parsing: https://github.com/scallop/scallop/wiki

Spark Resilient Distributed Dataset (RDD): https://spark.apache.org/docs/3.0.1/api/scala/org/apache/spark/rdd/RDD.html

# Credits

Erick Lavoie (Design, Implementation, Tests)

Athanasios Xygkis (Requirements, Tests)
