#Converting data to integer
from pyspark.sql.types import IntegerType

#First column requires original dataframe while new columns can be added to new dataframe
#_int subscript for dataframe with integer type while normal df is in string type
#collist is list of column names

collist = train_df.columns

train_dfint = train_df.withColumn(collist[0],train_df[collist[0]].cast(IntegerType()))
test_dfint = test_dfm.withColumn(collist[0],test_df[collist[0]].cast(IntegerType()))
for i in range(1,len(collist)):    #Here range starts from 1 i.e. second column
  train_dfint = train_dfint.withColumn(collist[i],train_df[collist[i]].cast(IntegerType()))
  test_dfint = test_dfint.withColumn(collist[i],test_df[collist[i]].cast(IntegerType()))
