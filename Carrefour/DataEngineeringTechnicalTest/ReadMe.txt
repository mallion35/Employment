1. Installation

On a machine running Windows 10, the following were installed:

(i) Python
(ii) PySpark
(iii) Java Runtime Environment
(iv) winutils (to provide some Linux-style commands that are invoked when executing SQL queries within PySpark)

The system PATH environment variable was adapted to include the location of the 'winutils.exe' executable.

A project directory 'C:\Carrefour\DataEngineeringTechnicalTest' was created, under which some sub-directories were created:

(i) 'Data/Input', for storing the Input Table used for testing.
(ii) 'Data/Output', for holding the Result Tables produced by testing.
(iii) 'Code', for housing the Python script 'ComputeResult.py' that performs the processing.

For testing, the following Input Table was used:

0, FA-14, FB-13, FC-11, 10
1, FA-1, FB-1, FC-1, 0
2, FA-13, FB-12, FC-10, 9
3, FA-2, FB-2, FC-2, 1
4, FA-12, FB-11, FC-9, 8
5, FA-3, FB-3, FC-3, 2
6, FA-11, FB-10, FC-8, 7
7, FA-4, FB-4, FC-4, 3
8, FA-10, FB-9, FC-7, 6
9, FA-5, FB-5, FC-5, 4
10, FA-14, FB-13, FC-11, 10
11, FA-1, FB-1, FC-1, 0
12, FA-13, FB-12, FC-10, 9
13, FA-2, FB-2, FC-2, 1
14, FA-12, FB-11, FC-9, 8


2. Execution

In the Windows PowerShell, navigate to the 'Code' sub-directory and execute the following command:

spark-submit .\ComputeResult.py


3. Results

The following Result table is obtained on screen:

+------------+-------------+-----+----------+
|Feature_Name|Feature_Value|Total|Percentage|
+------------+-------------+-----+----------+
|   Feature_A|         FA-2|    2|     0.026|
|   Feature_A|         FA-3|    2|     0.026|
|   Feature_A|         FA-4|    3|     0.039|
|   Feature_A|        FA-10|    6|     0.077|
|   Feature_A|        FA-11|    7|      0.09|
|   Feature_A|         FA-5|    4|     0.051|
|   Feature_A|        FA-13|   18|      0.23|
|   Feature_A|        FA-14|   20|     0.256|
|   Feature_A|        FA-12|   16|     0.205|
|   Feature_A|         FA-1|    0|       0.0|
|   Feature_B|         FB-9|    1|     0.067|
|   Feature_B|         FB-4|    1|     0.067|
|   Feature_B|         FB-5|    1|     0.067|
|   Feature_B|         FB-3|    1|     0.067|
|   Feature_B|        FB-10|    1|     0.067|
|   Feature_B|        FB-13|    2|     0.133|
|   Feature_B|         FB-2|    2|     0.133|
|   Feature_B|        FB-11|    2|     0.133|
|   Feature_B|         FB-1|    2|     0.133|
|   Feature_B|        FB-12|    2|     0.133|
|   Feature_C|         FC-2|    2|     0.026|
|   Feature_C|         FC-3|    2|     0.026|
|   Feature_C|         FC-7|    6|     0.077|
|   Feature_C|         FC-4|    3|     0.039|
|   Feature_C|         FC-8|    7|      0.09|
|   Feature_C|         FC-5|    4|     0.051|
|   Feature_C|        FC-10|   18|      0.23|
|   Feature_C|        FC-11|   20|     0.256|
|   Feature_C|         FC-9|   16|     0.205|
|   Feature_C|         FC-1|    0|       0.0|
+------------+-------------+-----+----------+

The corresponding CSV file is written below 'ResultTable' in the 'Output' subdirectory.

As required, all feature names appear in the result table and, for each feature, the sum of all of the Percentages is equal to 1.


4. Bonus Questions

(i) To make sure that, for any feature, the sum of all percentages is always exactly equal to 1, the Largest Remainder Method (see 'https://en.wikipedia.org/wiki/Largest_remainder_method') is invoked.
The method demands that the first few rows of a table be distinguished from the remaining rows. This could be accomplished by using the 'limit' method of the 'pyspark.sql.DataFrame' class.
Here, however, the 'zipWithIndex' method of the 'pyspark.RDD' class is used to accomplish the same objective (see 'ComputeResult.py' for details).

(ii) To allow new rules for computing the Total to be added easily, the code provides a dictionary associating each feature with the name of a function that computes the Total, and the definitions of the functions identified in the dictionary.
To add a new rule, the name of the corresponding function is associated with one or more features in the dictionary, and the definition of the function is added.

(iii) Processing the Input Table using PySpark SQL commands yields the following Result table:

+------------+-------------+-----+----------+
|Feature_Name|Feature_Value|Total|Percentage|
+------------+-------------+-----+----------+
|   Feature_B|        FB-10|    1|     0.067|
|   Feature_A|         FA-4|    3|     0.039|
|   Feature_C|         FC-1|    0|       0.0|
|   Feature_C|        FC-11|   20|     0.256|
|   Feature_A|        FA-10|    6|     0.077|
|   Feature_A|         FA-5|    4|     0.051|
|   Feature_B|         FB-4|    1|     0.067|
|   Feature_C|         FC-7|    6|     0.077|
|   Feature_B|         FB-9|    1|     0.067|
|   Feature_C|         FC-8|    7|      0.09|
|   Feature_C|         FC-4|    3|     0.039|
|   Feature_A|        FA-12|   16|     0.205|
|   Feature_A|        FA-14|   20|     0.256|
|   Feature_C|        FC-10|   18|      0.23|
|   Feature_A|        FA-13|   18|      0.23|
|   Feature_A|        FA-11|    7|      0.09|
|   Feature_A|         FA-3|    2|     0.026|
|   Feature_B|         FB-3|    1|     0.067|
|   Feature_A|         FA-1|    0|       0.0|
|   Feature_C|         FC-9|   16|     0.205|
|   Feature_C|         FC-2|    2|     0.026|
|   Feature_B|         FB-2|    2|     0.133|
|   Feature_A|         FA-2|    2|     0.026|
|   Feature_B|        FB-11|    2|     0.133|
|   Feature_C|         FC-5|    4|     0.051|
|   Feature_B|         FB-1|    2|     0.133|
|   Feature_C|         FC-3|    2|     0.026|
|   Feature_B|        FB-13|    2|     0.133|
|   Feature_B|        FB-12|    2|     0.133|
|   Feature_B|         FB-5|    1|     0.067|
+------------+-------------+-----+----------+

The corresponding CSV file is written below 'ResultTableSql' in the 'Output' subdirectory.

Apart from the ordering of the rows, the table is the same as that obtained previously, see above.


5. Miscellaneous

The code is designed in such a way that new features can easily be added. The number of features represented in the Input Table is determined automatically.
To add a new feature, the name of the feature and the name of the function that computes its Total must be added to the appropriate dictionary, see section 4(ii) above.