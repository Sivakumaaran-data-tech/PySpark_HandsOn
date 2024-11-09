from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder\
            .master("local[2]")\
            .appName("Merge_CSV")\
            .getOrCreate()

    custom_options = {
        "header": True,
        "inferSchema": True,
        "sep": ",",
        "quote": "\"",
        "escape": "\\",
        "mode": "PERMISSIVE"  # Error handling mode
    }

    us_vehicle_data = [(1,"BMW","i3","4Matic","US","AWD"),
                       (2,"Audi","A3 Quattro","Sport","US","RWD"),
                       (3,"Aston Martin","Rodge","F5","US","AWD"),
                       (4,"Toyota","Camry","Fleet","US","FWD"),
                       (5,"GMC","Sierra 1500","XLT","US","4WD")]

    us_columns = ["ID","make_name","model_name","sub_model_name","country","drive_type"]

    ca_vehicle_data = [(6, "Toyota", "Hyrider", "Sport", "CA"),
                       (7, "Audi", "A5 RS", "Sport", "CA"),
                       (8, "Mini", "Cooper", "Sport", "CA"),
                       (9, "Toyota", "Camry", "Fleet", "CA"),
                       (10, "GMC", "Sierra 1500", "XLT", "CA")]

    ca_columns = ["ID", "make_name", "model_name","sub_model_name", "country"]

    us_df = spark.createDataFrame(data=us_vehicle_data,schema=us_columns)
    ca_df = spark.createDataFrame(data=ca_vehicle_data, schema=ca_columns)

    # We can not union the above two different DFs. Because the difference in the count of the columns
    # union_df = us_df.union(ca_df)

    # To know the difference column names
    us_columns = set(us_df.columns)
    ca_columns = set(ca_df.columns)

    unique_columns = us_columns.symmetric_difference(ca_columns)

    print(unique_columns)

    # Try different union method

    union_df = us_df.unionByName(ca_df,allowMissingColumns=True)

    union_df.show()


