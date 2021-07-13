from transforms.api import configure, transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, ArrayType
from scipy import stats
import pandas as pd
from unitConversions import conversionsDictionary
import pyspark


@configure(profile=['NUM_EXECUTORS_16', 'EXECUTOR_MEMORY_LARGE'])
@transform_df(
    Output("/PATH/inferred_units_calculation"),
    codesetsLookup=Input("/PATH/concept_set_members"),
    measurements=Input("/PATH/measurement"),
    variables=Input("/PATH/canonical_units_of_measure")

)
def my_compute_function(variables, measurements, codesetsLookup, ctx):

    concepts = variables.join(codesetsLookup, 'codeset_id', 'inner')
    measurements_with_codesets = measurements.join(concepts, measurements.measurement_concept_id == concepts.concept_id, 'inner')

    df = measurements_with_codesets

    df = df \
        .withColumn('units', F.concat(F.col('unit_concept_name'), F.lit('~'), F.col('unit_concept_id'))) \
        .withColumnRenamed('measured_variable', 'MeasurementVar')

    unit_data_by_measurement_all = df.select('MeasurementVar',
                                             'codeset_id',
                                             'units',
                                             'data_partner_id',
                                             'unit_concept_name',
                                             'omop_unit_concept_name',
                                             'measurement_concept_name',
                                             'value_as_number',
                                             'max_acceptable_value',
                                             'min_acceptable_value')

    # filter out rows where value_as_number is null
    measurementsDF = unit_data_by_measurement_all \
        .where(F.col("value_as_number").isNotNull()) \
        .where(~F.isnan(F.col("value_as_number")))

    # perform a conversion to the canonical unit for all of the data points with a non-null and non-nmc unit to use as
    # the reference distribution, then re-check the K-S test for each unitless site distributions (applying each
    # conversion factor to the datapoints within it until a good K-S score is found --> becomes the designated unit)

    # ----------------------------------------------------------------------------------CREATE REFERENCE DATAFRAME------
    refDF = measurementsDF.where(F.col("units").isNotNull()) \
        .where(measurementsDF.unit_concept_name != 'No matching concept') \
        .where(measurementsDF.unit_concept_name != 'No information') \
        .where(measurementsDF.unit_concept_name != 'Other')

    # subset the reference values
    # Get a random sample of <=1000 rows per codeset_id
    # Ultimately we want sample size of <=100 rows, but need to account for the removal of implausible values after
    # the mapping function is applied. This avoids applying the mapping function to ALL rows only to choose 100 of them.
    w = W.partitionBy('codeset_id').orderBy(F.rand(42))
    refDF = refDF.select('*', F.rank().over(w).alias('rank')) \
                 .filter(F.col('rank') <= 1000) \
                 .drop('rank')

    refDF = refDF.withColumn('map_function', F.when(refDF.unit_concept_name == refDF.omop_unit_concept_name,
                                                    F.lit('x'))
                                              .otherwise(F.concat(F.col('unit_concept_name'),
                                                                  F.lit('_to_'),
                                                                  F.col('omop_unit_concept_name'),
                                                                  F.lit('_for_'),
                                                                  F.col('measurement_concept_name'))))

    # create a dictionary of conversion formulas for each map_function
    function_dict = conversionsDictionary.conversions
    # go through all the keys of the dictionary and if the map_function matches the column in measurement, add the
    # value of the dictionary key using lambda, else leave the value as it is
    refDF = refDF.coalesce(32)
    convertedUnit = F.when(F.lit(False), F.lit(""))
    for (function_name, mapping_function) in function_dict.items():
        convertedUnit = convertedUnit.when(
            F.col('map_function') == function_name,
            mapping_function(F.col('value_as_number')))
    refDF = refDF.withColumn("convertedUnit", convertedUnit.otherwise(F.lit("")))

    # cast to double type
    refDF = refDF.withColumn("convertedUnit", refDF["convertedUnit"].cast(DoubleType()))

    # null out implausible values
    refDF = refDF.withColumn("convertedUnit",
                             F.when(F.col('max_acceptable_value').isNotNull() &
                                    ((F.col('convertedUnit') > F.col('max_acceptable_value')) |
                                    (F.col('convertedUnit') < F.col('min_acceptable_value'))),
                                    None)
                             .otherwise(F.col('convertedUnit')))

    # Get a random sample of <=100 rows per codeset_id
    # This is the sample size we actually want AFTER implausible values have been removed.
    w = W.partitionBy('codeset_id').orderBy(F.rand(42))
    refDF = refDF.select('*', F.rank().over(w).alias('rank')) \
                 .filter(F.col('rank') <= 100) \
                 .drop('rank')

    # convertedUnit is now the reference canonical unit distribution that all
    # unknown unit value distributions will be tested against

    # ----------------------------------------------------------------------------------CREATE TEST DATAFRAME-----------
    # testDF if unit_concept_name is null, NMC, NI, or Other (ie: measurements without units)
    testDF = measurementsDF \
        .where((F.col("unit_concept_name").isNull()) |
               (measurementsDF.unit_concept_name == 'No matching concept') |
               (measurementsDF.unit_concept_name == 'No information') |
               (measurementsDF.unit_concept_name == 'Other')) \
        .drop('units')

    # Subset the test values
    # Get a random sample of <=100 rows per codeset_id+data_partner_id+measurement_concept_name
    w = W.partitionBy(['codeset_id', 'data_partner_id', 'measurement_concept_name']) \
         .orderBy(F.rand(42))
    testDF = testDF.select('*', F.rank().over(w).alias('rank')) \
                   .filter(F.col('rank') <= 100) \
                   .drop('rank')

    # --------------------------------------------------------------------------------PERFORM CONVERSIONS ON TEST DF----
    # map_function = unit_concept_name_TO_omop_unit_concept_name_FOR_measurement_concept_name
    # We only care about having a unique unit_concept_name_TO_omop_unit_concept_name per codeset_id
    uniqueConversions = refDF.select('codeset_id', 'units', 'map_function') \
        .dropDuplicates(subset=['codeset_id', 'units'])

    # Get conversions to perform on testDF values
    testDF = testDF.join(uniqueConversions, 'codeset_id')

    # go through all the keys of the dictionary and if the map_function matches the column in measurement,
    # add the value of the dictionary key using lambda, else leave the value as it is
    testDF = testDF.coalesce(32)
    convertedUnit = F.when(F.lit(False), F.lit(""))
    for (function_name, mapping_function) in function_dict.items():
        convertedUnit = convertedUnit.when(
            F.col('map_function') == function_name,
            mapping_function(F.col('value_as_number')))
    testDF = testDF.withColumn("convertedUnit", convertedUnit.otherwise(F.lit("")))

    # cast to double type
    testDF = testDF.withColumn("convertedUnit", testDF["convertedUnit"].cast(DoubleType()))

    # values by unit in testDF are now the test distributions for units that will be compared to the ref distribution

    # ---------------------------------------------------------------------------------PERFORM K-S TESTS--------------
    # check for converted unit
    @pandas_udf(ArrayType(DoubleType()))
    def get_ks_pval(refs, tests):
        pvalres = [stats.ks_2samp(refs[n], tests[n]) if ((len(refs[n]) > 20) and (len(tests[n]) > 20)) else [0.0, 0.0] for n in range(len(refs))]
        return pd.Series(pvalres)

    refDF = refDF \
        .groupBy('codeset_id') \
        .agg(F.collect_list("convertedUnit").alias('convertedUnitListRefs'))

    testDF = testDF.select('MeasurementVar',
                           'codeset_id',
                           'units',
                           'data_partner_id',
                           'measurement_concept_name',
                           'unit_concept_name',
                           'convertedUnit')

    testDF = testDF.join(refDF, 'codeset_id', 'left')
    testDF = testDF.groupBy('MeasurementVar',
                            'codeset_id',
                            'units',
                            'data_partner_id',
                            'measurement_concept_name',
                            'unit_concept_name') \
                   .agg(F.first('convertedUnitListRefs').alias('refVals'), F.collect_list('convertedUnit').alias('testVals'))

    # Applying pandas udf to the two columns to obtain p-value for each row
    testDF = testDF.withColumn("KSpval", get_ks_pval(testDF.refVals, testDF.testVals))

    ks_tests_per_site_null_unit = testDF.select('MeasurementVar',
                                                'codeset_id',
                                                'data_partner_id',
                                                'measurement_concept_name',
                                                'unit_concept_name',
                                                'units',
                                                'refVals',
                                                'testVals',
                                                testDF.KSpval[1].alias('KSpvalue'))

    return ks_tests_per_site_null_unit
