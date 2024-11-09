"TransformX: ETL Optimisation Through Lazy Transformation in BOSS" - a final year project thesis

It is an engine that is used within the BOSS ecosystem to create a data transformation pipeline that is being lazily executed only on the required data to save transformation costs. For example, suppose the default transformation pipeline would transform the whole dataset, but the user queries only a 5% portion of the data. In that case, the engine will transform only the required data portion during runtime. Proved to grant up to 3x performance benefits with complex data transformations and low tuple selectivity

For evaluation experiment the following engines were used:

BOSSKernelBenchmarks from main
-	Link: https://github.com/symbol-store/BOSSKernelBenchmarks/tree/main
-	Hash: acae81f

Updated the following files to suit my project (located in Benchmarks folder):

-	Updated Benchmarks/tpch.cpp
-	Updated Benchmarks/BOSSBenchmarks.cpp

BOSSArrowStorageEngine from int32_support_new_table_format branch. Unchanged
-	Link: https://github.com/symbol-store/BOSSArrowStorageEngine/tree/int32_support_new_table_format
-	Hash: 320b470

BOSSVeloxEngine from radix_joins branch. Unchanged
-	Link: https://github.com/symbol-store/BOSSVeloxEngine/tree/radix_joins
-	Hash: 3a3cdae

BOSS from main branch. Unchanged
-	Link: https://github.com/symbol-store/BOSS
-	Hash: 37fda7c
