from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, current_timestamp, date_format, expr, \
    collect_list


def get_insert_operation(column, alias):
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")).alias(alias)


def get_contract(df):
    contract_title = array(when(~isnull("legal_title_1"),
                                struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                       col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                           when(~isnull("legal_title_2"),
                                struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                       col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                           )

    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                            col("tax_id").alias("taxId")).alias("taxIdentifier")

    return df.select("account_id", get_insert_operation(col("account_id"), "contractIdentifier"),
                     get_insert_operation(col("source_sys"), "sourceSystemIdentifier"),
                     get_insert_operation(col("account_start_date").cast("string"), "contactStartDateTime"),
                     get_insert_operation(contract_title_nl, "contractTitle"),
                     get_insert_operation(tax_identifier, "taxIdentifier"),
                     get_insert_operation(col("branch_code"), "contractBranchCode"),
                     get_insert_operation(col("country"), "contractCountry"),
                     )


def get_relations(df):
    return df.select("account_id", "party_id",
                     get_insert_operation(col("party_id"), "partyIdentifier"),
                     get_insert_operation(col("relation_type"), "partyRelationshipType"),
                     get_insert_operation(col("relation_start_date").cast("string"), "partyRelationStartDateTime")
                     )


def get_address(df):
    address = struct(col("address_line_1").alias("addressLine1"),
                     col("address_line_2").alias("addressLine2"),
                     col("city").alias("addressCity"),
                     col("postal_code").alias("addressPostalCode"),
                     col("country_of_address").alias("addressCountry"),
                     col("address_start_date").cast("string").alias("addressStartDate")
                     )

    return df.select("party_id", get_insert_operation(address, "partyAddress"))


def join_party_address(p_df, a_df):
    return p_df.join(a_df, "party_id", "left_outer") \
        .groupBy("account_id") \
        .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress"
                                 ).alias("partyDetails")
                          ).alias("partyRelations"))


def join_contract_party(c_df, p_df):
    return c_df.join(p_df, "account_id", "left_outer")


from pyspark.sql.functions import col, lit, struct, expr, date_format, current_timestamp


# def apply_header(spark, df):
#     event_df = df.crossJoin(spark.createDataFrame([("SBDL-Contract", 1, 0)]) \
#                             .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion") \
#                             .withColumn("eventDateTime", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")))
#
#     # We create the eventHeader, keys, and payload structs
#     event_df = event_df.select(
#         struct(
#             expr("uuid()").alias("eventIdentifier"),
#             col("eventType"),
#             col("majorSchemaVersion"),
#             col("minorSchemaVersion"),
#             col("eventDateTime")
#         ).alias("eventHeader"),
#         array(
#             struct(
#                 lit("contractIdentifier").alias("keyField"),
#                 col("account_id").alias("keyValue")
#             )
#         ).alias("keys"),
#         struct(
#             col("contractIdentifier"),
#             col("sourceSystemIdentifier"),
#             col("contactStartDateTime"),
#             col("contractTitle"),
#             col("taxIdentifier"),
#             col("contractBranchCode"),
#             col("contractCountry"),
#             col("partyRelations")
#         ).alias("payload")
#     )
#
#     return event_df

def apply_header(spark, df):
    # Instead of a crossJoin, add the header and payload as new columns.
    # This is a much more efficient and stable operation.
    event_df = df.withColumn("eventHeader",
                            struct(
                                expr("uuid()").alias("eventIdentifier"),
                                lit("SBDL-Contract").alias("eventType"),
                                lit(1).alias("majorSchemaVersion"),
                                lit(0).alias("minorSchemaVersion"),
                                date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ").alias("eventDateTime")
                            )) \
        .withColumn("keys",
                    array(
                        struct(
                            lit("contractIdentifier").alias("keyField"),
                            col("account_id").alias("keyValue")
                        )
                    )) \
        .withColumn("payload",
                    struct(
                        col("contractIdentifier"),
                        col("sourceSystemIdentifier"),
                        col("contactStartDateTime"),
                        col("contractTitle"),
                        col("taxIdentifier"),
                        col("contractBranchCode"),
                        col("contractCountry"),
                        col("partyRelations")
                    ))

    # Select only the final columns needed for the Kafka message
    return event_df.select("eventHeader", "keys", "payload")