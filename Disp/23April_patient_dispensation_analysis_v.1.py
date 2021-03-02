#this code is used to calculate the consumption of ARVs across differnt facilities in states. The consumption can be calculated from three datasets
# dispenses_details+dispensation, stock_consumption, and adjustment_summary
import pandas as pd
import datetime
from datetime import datetime
import numpy as np
import os
#changing the working directory to 'Dispensation' folder
os.chdir("C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/Processed_files")
#selecting the number of columns to be visible for the data frame
pd.options.display.max_columns =30
#storing the path of the patient_master file,will vary across system, created on 31st March dataset, replace with latest dataset
path_patient_dispensation = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/patient_dispensation_31_march.csv"
#IMPORTING DATASET - reading the patient_master dataset
df_patient_dispensation=pd.read_csv(path_patient_dispensation,header = 0 )
