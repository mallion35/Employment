import sys
import pyspark
from pyspark.sql.types import *
from pyspark.sql import functions as F

# Since the program is run by spark-submit, a SparkContext variable must be created.
sc = pyspark.SparkContext('local[*]')

# Suppress much of the output of spark-submit.
sc.setLogLevel('WARN')

# Instantiate a SparkSession, using the existing SparkContext. It will be needed for creating dataframes and executing SQL queries.
spark = pyspark.sql.SparkSession(sc)

# Read a text file (the Input Table in CSV format) from the local file system as an RDD of strings.
# We don't create a dataframe directly because the number of features needs to be deduced before the schema can be created.
rdd = sc.textFile('C:\Carrefour\DataEngineeringTechnicalTest\Data\Input\InputTable.csv')

# Deduce the number of features and exit with an error message if this number exceeds the number of letters in the alphabet,
# i.e. only features Feature_A - Feature_Z are allowed.
NFeatures = len(rdd.take(1)[0].split(',')) - 2
if NFeatures > 26:
    sys.exit('Unsupported number of features in input table')

# Create a list of the features that are present in the Input Table.
featureList = []
for i in range(NFeatures):
    featureList.append('Feature_' + chr(65 + i))

# Build the corresponding schema.
schema = StructType([StructField('Id', StringType(), True)])
for feature in featureList:
    schema = schema.add(feature, StringType(), True)
schema = schema.add('N', IntegerType(), True)

# Convert the RDD to a dataframe, according to the derived schema.
# Leading white space must be ignored, since a space follows each comma in the Input Table.
df = spark.read.csv(rdd, schema=schema, ignoreLeadingWhiteSpace=True)

# Define functions that implement the various rules for calculating the Total.
# The variables 'df' and 'feature' defined elsewhere are visible within these functions.
def sumN():
    return df.groupBy(feature).agg(F.sum("N").alias("Total"))

def countId():
    return df.groupBy(feature).agg(F.count("Id").alias("Total"))

# Create a dictionary linking each feature with a rule for computing its total.    
Rules = {'Feature_A' : sumN, 'Feature_B' : countId, 'Feature_C' : sumN}

# Initialise a variable which will be used to aggregate the results obtained for the various features.
Results = None

# Iterate over the features.
for feature in featureList:
    # For the current feature, compute a total for each of its feature values.
    Tot = Rules[feature]()
    
    # For each feature value, calculate the fraction of the grand total (the sum of all of the totals) that its total represents.
    TotFrac = Tot.crossJoin(Tot.select(F.sum("Total").alias("Sum_total"))).withColumn("Fraction", F.col("Total") / F.col("Sum_total"))
    
    # For each feature value, multiply the fraction by 1000 and remove the decimal part to create an integer.
    TotFracInt = TotFrac.select(feature, "Total", "Fraction", (1000 * F.col("Fraction")).cast(IntegerType()).alias("Integer"))
    
    # Divide the integers derived above by 1000, to yield the fractions truncated to 3 decimal places.
    TotFracIntTrunc = TotFracInt.withColumn("TruncatedFraction", F.col("Integer") / 1000)
    
    # Calculate the fractional errors (in the fractions) induced by the truncation, and order the rows in descending order of these errors.
    TotFracIntTruncErr = TotFracIntTrunc.withColumn("FractionalError", F.when(F.col("Fraction") == 0, 0).otherwise((F.col("Fraction") - F.col("TruncatedFraction")) / F.col("Fraction"))).orderBy("FractionalError", ascending=0)
    
    # Convert the above dataframe to an RDD, add an index, and convert back to a dataframe.
    TotFracIntTruncErrIndexed = TotFracIntTruncErr.rdd.zipWithIndex().toDF()
    
    # Repair the dataframe, reinstating the desired columns.
    for column in TotFracIntTruncErr.schema.names:
        TotFracIntTruncErrIndexed = TotFracIntTruncErrIndexed.withColumn(column, TotFracIntTruncErrIndexed['_1'].getItem(column))
    
    # Drop the first column, which is now superfluous, and correctly identify the next column as the Index.
    TotFracIntTruncErrIndexed = TotFracIntTruncErrIndexed.drop("_1")
    TotFracIntTruncErrIndexed = TotFracIntTruncErrIndexed.withColumnRenamed('_2', 'Index')
    
    # Sum the integers generated earlier, storing the result in a Python variable.
    # The sum should be less than 1000, due to the way in which the integers were generated.
    IntTot = TotFracIntTruncErrIndexed.groupBy().sum("Integer").collect()[0]["sum(Integer)"]
    
    # Increment the first few integers (those corresponding to the greatest fractional errors in the fractions), to produce a new column of integers which sum to exactly 1000.
    TotFracIntTruncErrIndexedModInt = TotFracIntTruncErrIndexed.withColumn("ModifiedInteger", F.when(F.col("Index") >= 1000 - IntTot, F.col("Integer")).otherwise(F.col("Integer") + 1))
    
    # Divide the new integers by 1000, to yield a set of fractions which sum exactly to 1.
    # These fractions are not strictly percentages, but are nevertheless denoted "percentages" in the terminology of the Test.
    TotFracIntTruncErrIndexedModIntPerc = TotFracIntTruncErrIndexedModInt.withColumn("Percentage", F.col("ModifiedInteger") / 1000)
    
    # Tidy up the table by dropping unwanted columns.
    TotPerc = TotFracIntTruncErrIndexedModIntPerc.drop("Index").drop("Fraction").drop("Integer").drop("TruncatedFraction").drop("FractionalError").drop("ModifiedInteger")
    
    # Prepare the current table for amalgamation with the results tables for the remaining features.
    # Namely, the column bearing the name of the current feature is renamed to 'Feature_Value', and a new column 'Feature_Name' is added.
    # This latter column simply contains the name of the current feature in each row.
    FeatValTotPerc = TotPerc.withColumnRenamed(feature, 'Feature_Value')
    FeatNamFeatValTotPerc = FeatValTotPerc.select(F.lit(feature).alias("Feature_Name"), "Feature_Value", "Total", "Percentage")
    
    # If the current feature is the first, store the results for the current feature.
    # Otherwise, amalgamate the results for the current feature with the existing results.
    if Results is None:
        Results = FeatNamFeatValTotPerc
    else:
        Results = Results.union(FeatNamFeatValTotPerc)

# For verification purposes, display up to 100 records of the Results table on the driver node.
# In the present case, this allows the results to be visualised completely.
Results.show(n=100)

# Also for verification purposes, write the complete Results table to a CSV file on the local file system.
Results.coalesce(1).write.csv('C:\Carrefour\DataEngineeringTechnicalTest\Data\Output\ResultTable', header=True)

# ------------------------------------------------------------------------------------------------
# Now we repeat the exercise using SQL.

# For the dataframe corresponding to the Input Table, create a temporary view on which SQL queries may be executed.
df.createOrReplaceTempView("df")

# Define functions that implement the various rules for calculating the Total.
# The table name 'df' and variable 'feature' defined elsewhere are visible within these functions.        
def sumNSql():
    return spark.sql("SELECT %s, SUM(N) AS Total from df GROUP BY %s" % (feature, feature))

def countIdSql():
    return spark.sql("SELECT %s, COUNT(Id) AS Total from df GROUP BY %s" % (feature, feature))

# Create a dictionary linking each feature with a rule for computing its total.    
RulesSql = {'Feature_A' : sumNSql, 'Feature_B' : countIdSql, 'Feature_C' : sumNSql}

# Reinitialise the variable which is used to aggregate the results obtained for the various features.
Results = None

# Iterate over the various features.
for feature in featureList:

    # For the current feature, compute a total for each of its feature values, and create a new temporary view.
    Tot = RulesSql[feature]()
    Tot.createOrReplaceTempView("Tot")
    
    # For each feature value, calculate the fraction of the grand total (the sum of all of the totals) that its total represents, and create a new temporary view.
    TotFrac = spark.sql("SELECT *, Total / (SELECT SUM(Total) FROM Tot) AS Fraction FROM Tot")
    TotFrac.createOrReplaceTempView("TotFrac")
    
    # For each feature value, multiply the fraction by 1000 and remove the decimal part to create an integer. Create a new temporary view.
    TotFracInt = spark.sql("SELECT *, CAST(1000 * Fraction AS int) AS Integer FROM TotFrac")
    TotFracInt.createOrReplaceTempView("TotFracInt")
    
    # Divide the integers derived above by 1000, to yield the fractions truncated to 3 decimal places. Create a new temporary view.
    TotFracIntTrunc = spark.sql("SELECT *, Integer / 1000 AS TruncatedFraction FROM TotFracInt")
    TotFracIntTrunc.createOrReplaceTempView("TotFracIntTrunc")
    
    # Calculate the fractional errors (in the fractions) induced by the truncation, and order the rows in descending order of these errors. Create a new temporary view.
    TotFracIntTruncErr = spark.sql("SELECT *, CASE WHEN Fraction = 0 THEN 0 ELSE (Fraction - TruncatedFraction) / Fraction END AS FractionalError FROM TotFracIntTrunc ORDER BY FractionalError DESC")
    TotFracIntTruncErr.createOrReplaceTempView("TotFracIntTruncErr")
    
    # Sum the integers generated earlier, storing the result in a Python variable.
    # The sum should be less than 1000, due to the way in which the integers were generated.
    IntTot = spark.sql("SELECT SUM(Integer) AS SumInt FROM TotFracIntTruncErr").collect()[0]["SumInt"]
    
    # From the table in which the fractional errors were introduced, take the first few records - those for which the fraction will need to be adapted. Create a new temporary view.
    TotFracIntTruncErrTop = spark.sql("SELECT * FROM TotFracIntTruncErr LIMIT 1000 - %s" % str(IntTot))
    TotFracIntTruncErrTop.createOrReplaceTempView("TotFracIntTruncErrTop")
    
    # Get the total number of records in the table in which the fractional errors were introduced.
    CountVal = spark.sql("SELECT COUNT(%s) AS Count FROM TotFracIntTruncErr" % feature).collect()[0]["Count"]
    
    # Reverse the order of the table in which the fractional errors were introduced, and create a temporary view of the resulting table.
    TotFracIntTruncErrRev = spark.sql("SELECT * FROM TotFracIntTruncErr ORDER BY FractionalError")
    TotFracIntTruncErrRev.createOrReplaceTempView("TotFracIntTruncErrRev")
    
    # Take records from the top of the above table, so as to harvest the outstanding records from the table in which the fractional errors were introduced.
    # Create a corresponding temporary view.
    TotFracIntTruncErrBottom = spark.sql("SELECT * FROM TotFracIntTruncErrRev LIMIT %s - 1000" % str(CountVal + IntTot))
    TotFracIntTruncErrBottom.createOrReplaceTempView("TotFracIntTruncErrBottom")
    
    # For each fraction that is to be adapted, provide an integer that is one greater than the integer calculated previously. Create a new temporary view.
    TotFracIntTruncErrTopModInt = spark.sql("SELECT *, Integer + 1 AS ModifiedInteger FROM TotFracIntTruncErrTop")
    TotFracIntTruncErrTopModInt.createOrReplaceTempView("TotFracIntTruncErrTopModInt")
    
    # For each fraction that is not to be adapted, provide an integer that is equal to the integer calculated previously. Create a new temporary view.
    TotFracIntTruncErrBottomModInt = spark.sql("SELECT *, Integer AS ModifiedInteger FROM TotFracIntTruncErrBottom")
    TotFracIntTruncErrBottomModInt.createOrReplaceTempView("TotFracIntTruncErrBottomModInt")
    
    # Combine the table containing the adapted integers with the table containing the duplicate integers. The adapted and duplicate integers should sum to exactly 1000.
    # Create a new temporary view.
    TotFracIntTruncErrModInt = spark.sql("SELECT * FROM TotFracIntTruncErrTopModInt UNION SELECT * FROM TotFracIntTruncErrBottomModInt")
    TotFracIntTruncErrModInt.createOrReplaceTempView("TotFracIntTruncErrModInt")
    
    # Divide the adapted and duplicate integers by 1000, to yield a set of fractions which sum exactly to 1.
    # These fractions are not strictly percentages, but are nevertheless denoted "percentages" in the terminology of the Test.
    # Create a new temporary view.
    TotFracIntTruncErrModIntPerc = spark.sql("SELECT *, ModifiedInteger / 1000 AS Percentage FROM TotFracIntTruncErrModInt")
    TotFracIntTruncErrModIntPerc.createOrReplaceTempView("TotFracIntTruncErrModIntPerc")
    
    # Prepare the current table for amalgamation with the results tables for the remaining features.
    # Namely, change the name of the column bearing the name of the current feature to 'Feature_Value',
    # add a new column 'Feature_Name' containing the name of the current feature in each row, and, from the remaining columns, select only those of interest.
    # Create a new temporary view.
    FeatNamFeatValTotPerc = spark.sql("SELECT '%s' AS Feature_Name, %s AS Feature_Value, Total, Percentage FROM TotFracIntTruncErrModIntPerc" % (feature, feature))
    FeatNamFeatValTotPerc.createOrReplaceTempView("FeatNamFeatValTotPerc")
    
    # If the current feature is the first, store the results for the current feature.
    # Otherwise, amalgamate the results for the current feature with the existing results.
    if Results is None:
        Results = FeatNamFeatValTotPerc
    else:
        Results = spark.sql("SELECT * FROM Results UNION SELECT * FROM FeatNamFeatValTotPerc")
    
    # Create a temporary view corresponding to the results gathered so far.
    Results.createOrReplaceTempView("Results")

# For verification purposes, display up to 100 records of the Results table on the driver node.
# In the present case, this allows the results to be visualised completely.
Results.show(n=100)

# Also for verification purposes, write the complete Results table to a CSV file on the local file system.
Results.coalesce(1).write.csv('C:\Carrefour\DataEngineeringTechnicalTest\Data\Output\ResultTableSql', header=True)
