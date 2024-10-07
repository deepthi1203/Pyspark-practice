# Databricks notebook source
df = spark.read.format('csv').option("header",True).load('/mnt/landing/AllFiles/*.csv')
display(df)

# COMMAND ----------

adls_path = "/mnt/landing/AllFiles"
all_files = dbutils.fs.ls(adls_path)

#filter out csv files
csv_files = [file.path for file in all_files if file.path.endswith(".csv")]
dataframes = {}

#loop over each csv file path and load each into a sepearte dataframe
for file_path in csv_files:
    #extract the file name without extension to use as a dictionary key
    file_name = file_path.split("/")[-1].replace(".csv","")
    #read each csv file into a sepearte dataframe
    df = spark.read.csv(file_path,header=True)
    #store the dataframe in the dictionary using the file name as a key
    dataframes[file_name] = df
    #display(df)
#look for itterating each dict item    
for file_name, df in dataframes.items():
    #writing into a seperate dataframe
    df.write.format('csv').mode("overwrite").option("header",True).\
        save('/mnt/landing/AllFilesOutput/' + file_name)
print(dataframes.keys())           
