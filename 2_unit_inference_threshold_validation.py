def unit_data_by_measurement_all(Filter_measurement_with_codeset_alias):
    from pyspark.sql.functions import concat, col, lit

    df = Filter_measurement_with_codeset_alias

    df = df.withColumn('units', F.concat(F.col('unit_concept_name'),F.lit('~'), F.col('unit_concept_id')))
    df = df.withColumnRenamed('measured_variable','MeasurementVar')

    df = df.select('MeasurementVar','codeset_id','units','data_partner_id','unit_concept_name','omop_unit_concept_name','measurement_concept_name','value_as_number','max_acceptable_value','min_acceptable_value')

    sampFrac = 1500000000 / df.count() 
    df = df.sample(withReplacement=False, fraction=sampFrac, seed=42)

    return df
    
def ks_tests_per_site_null_unit(unit_data_by_measurement_all):
    unit_data_by_measurement_all = unit_data_by_measurement_all
    from scipy import stats
    import numpy as np
    from pyspark.sql.functions import col, when, isnan
    from pyspark.sql.functions import pandas_udf, PandasUDFType
    from pyspark.sql.types import DoubleType, ArrayType
    import pandas as pd
    from unitConversions import conversionsDictionary
    import itertools 
    from itertools import permutations

    # create a dictionary of conversion formulas for each map_function
    function_dict = conversionsDictionary.conversions # add the actual dictionary if this doesn't work

    measurementsDF = unit_data_by_measurement_all

    measurementsDF = measurementsDF.where(col("value_as_number").isNotNull())
    measurementsDF = measurementsDF.where(~isnan(col("value_as_number")))
    # nanDF = measurementsDF.where(isnan(col("value_as_number")))

    # perform a conversion to the canonical unit for all of the data points with a non-null and non-nmc unit to use as the reference distribution, then re-check the K-S test for each unitless site distributions (applying each conversion factor to the datapoints within it until a good K-S score is found --> becomes the designated unit)

    # filter out rows where units is null
    refDF = measurementsDF.where(col("units").isNotNull())
    refDF = refDF.where(refDF.unit_concept_name != 'No matching concept')
    #refDF = refDF.withColumn("convertedUnit", apply_map_udf("value_as_number", "Accepted_formula_consolidated"))

    # add on a new column harmonized_value_as_number and initialize it for every row with null (empty string)
    refDF = refDF.withColumn('map_function', F.when(refDF.unit_concept_name == refDF.omop_unit_concept_name,F.lit('x')).otherwise(F.concat(F.col('unit_concept_name'),F.lit('_to_'), F.col('omop_unit_concept_name'),F.lit('_for_'),F.col('measurement_concept_name'))))
    
    refDF = refDF.withColumn("convertedUnit", F.lit(""))
    # go through all the keys of the dictionary and if the map_function matches the column in measurement, add the value of the dictionary key using lambda, else leave the value as it is (either null or previously evaluated value from a different map_function)
    for function_name, mapping_function in function_dict.items():
        refDF = refDF.withColumn("convertedUnit", F.when(refDF.map_function == function_name,mapping_function(refDF.value_as_number)).otherwise(refDF.convertedUnit))
    
    # cast to double type
    refDF = refDF.withColumn("convertedUnit", refDF["convertedUnit"].cast(DoubleType()))
    #refDF = refDF.drop("map_function")

    #convertedUnit is now the reference canonical unit distribution that all unknown unit value distributions will be tested against

    # subset the reference values by variable (speeds up computation and ensures KS test is performed on similar sized arrays)

    codesetlist = [x.codeset_id for x in refDF.select('codeset_id').distinct().collect()]
    print(codesetlist)  
    frac_dictionary = dict.fromkeys(codesetlist)
    for k in frac_dictionary.keys():
        print(k)
        if refDF.filter(refDF.codeset_id == F.lit(k)).count() > 100:
            print('codeset greater')
            frac_dictionary[k] = (100 / refDF.filter(refDF.codeset_id == F.lit(k)).count())
        else:
            frac_dictionary[k] = 1
            print(refDF.filter(refDF.codeset_id == F.lit(k)).count())
    print(frac_dictionary)
    refDF = refDF.sampleBy("codeset_id", fractions=frac_dictionary, seed=42)

    # TREAT REFERENCE AS A TEST! OBTAIN RECORDS WHERE UNIT IS PRESENT, MOVE THE UNIT COL TO NEW COL (FOR COMPARISON), AND NULL OUT THE ORIGINAL UNIT COLUMN 
    refDF_test = measurementsDF.where(col("units").isNotNull())
    refDF_test = refDF_test.where(refDF_test.unit_concept_name != 'No matching concept')
    refDF_test = refDF_test.withColumn("ref_units", refDF_test.units)
    refDF_test = refDF_test.withColumn("ref_unit_concept_name", refDF_test.unit_concept_name)
    unitless = refDF_test.withColumn("unit_concept_name", F.lit("")).drop('units')

    # subset the test values by codeset, data partner, measurement_concept_name and ref_unit_concept_name (speeds up computation and ensures KS test is performed on similar sized arrays)

    @pandas_udf(DoubleType())
    def return_frac(grp_counts):
        frac_res = [(100/grp_counts[n]) if (grp_counts[n] > 100) else 1 for n in range(len(grp_counts))]
        return pd.Series(frac_res)

    unitlessGroupCounts = unitless.groupBy('codeset_id','data_partner_id','measurement_concept_name','ref_unit_concept_name').count().withColumnRenamed('count','group_count')
    unitlessGroupCounts = unitlessGroupCounts.withColumn("frac", return_frac(unitlessGroupCounts.group_count))
    unitlessGroupCounts = unitlessGroupCounts.withColumnRenamed('codeset_id','codesetID')
    unitlessGroupCounts = unitlessGroupCounts.withColumnRenamed('data_partner_id','data_partnerID')
    unitlessGroupCounts = unitlessGroupCounts.withColumnRenamed('measurement_concept_name','measurement_concept')
    unitlessGroupCounts = unitlessGroupCounts.withColumnRenamed('ref_unit_concept_name','ref_unit_names')

    unitless = unitless.join(unitlessGroupCounts,((unitless.codeset_id == unitlessGroupCounts.codesetID) & (unitless.data_partner_id == unitlessGroupCounts.data_partnerID) & (unitless.measurement_concept_name == unitlessGroupCounts.measurement_concept) & (unitless.ref_unit_concept_name == unitlessGroupCounts.ref_unit_names)),'left').drop('codesetID','data_partnerID','measurement_concept', 'ref_unit_names')


    fracs = unitlessGroupCounts.rdd.map(lambda x: (x[0],x[1],x[2],x[3],x[5])).distinct().map(lambda x: (x,x[4])).collectAsMap()
    print(fracs)

    #setup how to key the dataframe
    kb = unitless.rdd.keyBy(lambda x: (x[1],x[2],x[5],x[10],x[12]))

    sampled = kb.sampleByKey(False,fracs).map(lambda x: x[1]).toDF(unitless.schema)
    sampled = sampled.drop('group_count','frac')
    
    ###
    refDF = refDF.repartition(1)
    uniqueConversions = refDF.select('codeset_id','units','map_function').withColumnRenamed('codeset_id','codeset')
    uniqueConversionsUniqueDeDup = uniqueConversions.dropDuplicates(subset=['codeset','units'])

    unitlessConverted = sampled.crossJoin(uniqueConversionsUniqueDeDup)
    unitlessConverted = unitlessConverted.filter(unitlessConverted.codeset == unitlessConverted.codeset_id)

    ### ADD ON THE CONVERTED VALUES FOR THE UNITLESS MEASUREMENTS ###

    unitlessConverted = unitlessConverted.withColumn("convertedUnit", F.lit("")).drop('codeset')
    # go through all the keys of the dictionary and if the map_function matches the column in measurement, add the value of the dictionary key using lambda, else leave the value as it is (either null or previously evaluated value from a different map_function)
    for function_name, mapping_function in function_dict.items():
        unitlessConverted = unitlessConverted.withColumn("convertedUnit", F.when(
                unitlessConverted.map_function == function_name,
                mapping_function(unitlessConverted.value_as_number)
            ).otherwise(unitlessConverted.convertedUnit)
        )

    # cast to double type
    unitlessConverted = unitlessConverted.withColumn("convertedUnit", unitlessConverted["convertedUnit"].cast(DoubleType()))
    unitlessConverted = unitlessConverted.drop("map_function")

    unitlessConverted = unitlessConverted.filter(F.col('convertedUnit').isNotNull()) # many sites have junk for their unit

    # add another column to indicate whether a conversion other than a 1:1 conversion was applied 
    unitlessConverted = unitlessConverted.withColumn("sameUnitFlag", F.when(unitlessConverted.value_as_number == unitlessConverted.convertedUnit,F.lit("Y")).otherwise(F.lit("N")))

    #values by unit in unitlessConverted are now the test distributions for units that will be compared to the reference distribution

    ## perform K-S tests
    ## check for converted unit
    @pandas_udf(ArrayType(DoubleType()))
    def return_ks_pval(refs,tests):

        pvalres = [stats.ks_2samp(refs[n],tests[n]) if ((len(refs[n]) > 20) and (len(tests[n]) > 20)) else [0.0,0.0] for n in range(len(refs))]
        return pd.Series(pvalres)


    unitlessConverted = unitlessConverted.select('MeasurementVar','codeset_id','units','data_partner_id','measurement_concept_name','ref_units','ref_unit_concept_name','unit_concept_name','convertedUnit','sameUnitFlag','value_as_number')

    refDF = refDF.groupBy('codeset_id').agg(F.collect_list("convertedUnit").alias('convertedUnitListRefs'))
    unitlessConverted = unitlessConverted.join(refDF,'codeset_id','left')
    unitlessConverted = unitlessConverted.groupBy('MeasurementVar','codeset_id','units','data_partner_id','measurement_concept_name','ref_units','ref_unit_concept_name','unit_concept_name').agg(F.first('convertedUnitListRefs').alias('refVals'), F.first('sameUnitFlag').alias('sameFlag'), F.collect_list('convertedUnit').alias('testVals'),F.collect_list('value_as_number').alias('originalVals'))

    print('applying pandas udf to the two columns to obtain p-value for each row')
    unitlessConverted = unitlessConverted.withColumn("KSpval", return_ks_pval(unitlessConverted.refVals, unitlessConverted.testVals))

    unitlessConverted = unitlessConverted.select('MeasurementVar','codeset_id','data_partner_id','measurement_concept_name','ref_units','ref_unit_concept_name','unit_concept_name','units','refVals','testVals','originalVals','sameFlag',unitlessConverted.KSpval.alias('KSres'),unitlessConverted.KSpval[1].alias('KSpvalue'))

    return unitlessConverted
    
def real_vs_estimated_units(ks_tests_per_site_null_unit, Conversions_table_filtered):
    from pyspark.sql.functions import concat_ws,col
    from pyspark.sql.window import Window
    conversions = Conversions_table_filtered
    df = ks_tests_per_site_null_unit

    split_col_refs = F.split(df['ref_units'], '~')
    split_col_inferred = F.split(df['units'], '~')

    df = df.withColumn('REFunit',split_col_refs.getItem(0))
    df = df.withColumn('INFERREDunit',split_col_inferred.getItem(0))

    conversions = conversions.select('measured_variable','unit','conversion_formula','harmonized_unit').distinct()
    
    df = df.join(conversions,((df.MeasurementVar == conversions.measured_variable) & (df.REFunit == conversions.unit)),"left")
    df = df.withColumnRenamed('conversion_formula','conversion_formula_refs')
    df = df.withColumnRenamed('harmonized_unit','harmonized_unit_refs')
    df = df.drop('unit','measured_variable')

    df = df.join(conversions,((df.MeasurementVar == conversions.measured_variable) & (df.INFERREDunit == conversions.unit)),"left")
    df = df.withColumnRenamed('conversion_formula','conversion_formula_inferred')
    df = df.withColumnRenamed('harmonized_unit','harmonized_unit_inferred')
    df = df.drop('unit','measured_variable')

    df = df.withColumn('harmonized_unit',F.coalesce(F.col('harmonized_unit_refs'),F.col('harmonized_unit_inferred')))

    df = df.drop('harmonized_unit_refs','harmonized_unit_inferred')

    df = df.withColumn('conversion_formula_refs',F.when(df.REFunit == df.harmonized_unit,'x').otherwise(F.col('conversion_formula_refs')))
    df = df.withColumn('conversion_formula_inferred',F.when(df.INFERREDunit == df.harmonized_unit,'x').otherwise(F.col('conversion_formula_inferred')))

    df = df.withColumn('identity',F.when((df.conversion_formula_refs != 'None') & (df.conversion_formula_inferred != 'None') & (df.conversion_formula_refs == df.conversion_formula_inferred),'identity').otherwise('non-identity'))
    df = df.withColumn('identity',F.when(df.REFunit == df.INFERREDunit,'identity').otherwise(F.col('identity')))

    df = df.filter(~(((df.conversion_formula_refs == 'None') | (df.conversion_formula_inferred == 'None')) & (F.col('identity') == 'non-identity')))

    return df 
