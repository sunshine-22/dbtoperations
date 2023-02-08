from pyspark.sql.types import StringType,ArrayType,StructType
from pyspark.sql.functions import col,explode,split,arrays_zip,coalesce


def model(dbt,session):
    dbt.config(materialized = "table")
    data_frame=dbt.ref("src_dfc")
    data_df=(data_frame.withColumn('patients_patientid', col('col.0'))
           .withColumn('patients_petid', col('col.1'))
           .withColumn('patients_carelocation_ouid', col('col.2'))
           .withColumn('patients_name', coalesce(col('col.3'), col('col.4')))
           .withColumn('patients_animaltype_animalclassname', col('col.5'))
           .withColumn('patients_animaltype_animalclass', col('col.6'))
           .withColumn('patients_animaltype_animalbreed', col('col.7'))
           .withColumn('patients_animaltype_animalbreedname', col('col.8'))
          )
    data_df = data_df.drop('col')
    # data_df=data_df.groupby(*['patients_patientid','patients_petid','patients_carelocation_ouid','patients_name','patients_animaltype_animalclassname','patients_animaltype_animalclass','patients_animaltype_animalbreed','patients_animaltype_animalbreedname'])
    return data_df
