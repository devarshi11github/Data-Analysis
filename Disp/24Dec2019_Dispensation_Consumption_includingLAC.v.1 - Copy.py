# calculate the consumption taking place at a facility level, this is to cater to the Scorecard error taking place in the IMS consumption pattern
#Total consumption will comprise of the consumption at facility (sinigle dispensation), LAC bulk consumption and stock adjustment
import pandas as pd
import math
import matplotlib.pyplot as plt
import random
from datetime import datetime, timedelta
import numpy as np
import os
import glob
#changing the working directory
os.chdir("C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/Processed_files")
pd.options.display.max_columns =40
#the dispensation component will comprise of three sources, direct dispensation, LAC bulk consumption and stock adjustment,
#creating segments for them,starting with direct dispensation which is available through table, patient_dispense_details and patient_dispensation
#there is an issue in downloading the complete dispensation anddispense details table, so download in parts and merge them
# storing the path for patient_dispensation file, this file has the fac_loc_id where the dispensation took place
#path_patient_dispensation = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/Archive/Dec19/patient_dispensation_1Dec.csv"
#loading the patient_dispensation data - reading entire file at one go may lead to memory error use iterator approach
#df_patient_dispensation = pd.read_csv(path_patient_dispensation,header = 0)

#merging the different dispensation files (year wise breakup), since there is an issue in downloading  the complete file, there is a variation of 10Mn between single downloaded file verses merged files
path_dispensation_merge = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/Patient_Dispensation"
all_dispensation_files=glob.glob(path_dispensation_merge + "/*.csv") #searches for all the csv file in the designated folder
li = []
for filename in all_dispensation_files:
    df1 = pd.read_csv(filename,index_col=None, header=0)
    li.append(df1)

df_patient_dispensation=pd.concat(li,axis=0,ignore_index=True) #Combined patient_dispensation data
#removing the duplicate dispense_ids (the files which are merged have time periods which overlap, hence keeping the unique dispense_ids)
df_patient_dispensation['dispense_id'].nunique()
df_patient_dispensation['dispense_id'].count()
df_patient_dispensation['dispense_id'].size
#removing the duplicate dispens_id from  the patient_dispensation table
df_patient_dispensation=df_patient_dispensation.drop_duplicates(subset='dispense_id',keep='last')

#the patient dispensation dataset is to be connected to the patient_dispence_details to get the fac_loc_id and dispense date  
#storing the path for patient dispense details, this table has the given quantity
#path_patient_dispense_detail = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/patient_dispence_details_1Dec.csv"
#df_patient_dispense_detail = pd.read_csv(path_patient_dispense_detail,header = 0)

#merging the different dispense details files (year wise breakup), since there is an issue in downloading  the complete file, there is a variation of 10Mn between single downloaded file verses merged files
path_dispense_detail_merge = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/5.Dispensation/Patient_Dispense_details"
all_dispense_detail_files=glob.glob(path_dispense_detail_merge + "/*.csv") #getting list of all csv files in the designated folder
mi = []
for filename in all_dispense_detail_files:
    df2 = pd.read_csv(filename,index_col=None, header=0)
    mi.append(df2)

df_patient_dispense_detail=pd.concat(mi,axis=0,ignore_index=True) #Combined patient_dispensation data
df_patient_dispense_detail=df_patient_dispense_detail.drop_duplicates(subset='dispence_id',keep='last')

#from patient_dispensation add the fac_loc_id and dispence_date to patient_dispense_details table
df_patient_dispense_detail.insert(3,'fac_loc',df_patient_dispense_detail['dispence_id'].map(df_patient_dispensation.set_index('dispense_id')['fac_loc_id']))
df_patient_dispense_detail.insert(4,'dispense_date',df_patient_dispense_detail['dispence_id'].map(df_patient_dispensation.set_index('dispense_id')['dispence_date']))
#importing location file, this file has IMS location data mapped to the MPR ART Code updated as on 31st August
path_location = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/4.Facility Master/31Aug2019_facility_location.csv"
facility_location = pd.read_csv(path_location,header = 0)
#entering the state master dataset and SACS master dataset
path_state = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/4.Facility Master/31Aug2019_master_states.csv"
facility_state = pd.read_csv(path_state,header = 0)
#importing the SACS to the loation file
path_sacs = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/4.Facility Master/31Aug2019_sacs_location.csv"
facility_sacs = pd.read_csv(path_sacs,header = 0)
#importing the district into the location file
path_district = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/4.Facility Master/1Dec2019_master_districts.csv"
facility_district = pd.read_csv(path_district,header = 0)
#add the State, SACS, and district in the facility_location dataset
facility_location.insert(10,'State_name',facility_location['state'].map(facility_state.set_index('state_id')['state_name']))
facility_location.insert(2,'SACS',facility_location['sacs_loc_id'].map(facility_sacs.set_index('loc_id')['loc_name']))
facility_location.insert(3,'District',facility_location['district_id'].map(facility_district.set_index('district_id')['district_name']))

#mapping the location / geographic (State) data from facility_location dataframe to df_patient_dispense_detail, this will help in getting the dispensation at location level
df_patient_dispense_detail.insert(4,'State',df_patient_dispense_detail['fac_loc'].map(facility_location.set_index('fac_id')['State_name']))
df_patient_dispense_detail.insert(5,'SACS',df_patient_dispense_detail['fac_loc'].map(facility_location.set_index('fac_id')['SACS']))
df_patient_dispense_detail.insert(6,'Facility Name',df_patient_dispense_detail['fac_loc'].map(facility_location.set_index('fac_id')['fac_name']))
df_patient_dispense_detail.insert(7,'MPR Code',df_patient_dispense_detail['fac_loc'].map(facility_location.set_index('fac_id')['art_code']))
df_patient_dispense_detail.insert(8,'District_Name',df_patient_dispense_detail['fac_loc'].map(facility_location.set_index('fac_id')['District']))

#adding the product_id to the table df_patient_dispense_detail
path_product = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/3.Product Master/product_master_Dec19.csv"
product_master = pd.read_csv(path_product,header = 0)
#inserting the product_name in  the df_patient_dispense_detail
df_patient_dispense_detail.insert(7,'Product_Name',df_patient_dispense_detail['product_id'].map(product_master.set_index('product_id')['product_name']))
# creating a 2-D table where rows have State, district, and facility name and the columns have name of drug and month for quantity consumed
#first step is grouping the data and having the pd.Grouper option
df_patient_dispense_detail['dispense_date'] = pd.to_datetime(df_patient_dispense_detail['dispense_date'], errors = 'coerce')

df_patient_dispense_detail_group = df_patient_dispense_detail.groupby(['State','SACS','District_Name','MPR Code','Facility Name',pd.Grouper(key = 'dispense_date',freq = 'Y')])['dispence_id'].count().reset_index().sort_values('dispense_date')
df_patient_dispense_detail_group['dispense_date'] = df_patient_dispense_detail_group['dispense_date'].dt.date
#to be further refined to have sum of dispensation as column name and date having no time
df_patient_dispense_detail_pivot = pd.pivot_table(df_patient_dispense_detail_group,index = ['State','SACS','District_Name','MPR Code','Facility Name'], columns = ['dispense_date'], fill_value =0, margins = True,margins_name='Total_dispensation').reset_index()
#total column is showing average please check also there is mismatch in number please check, this is for count of number of dispensation
df_patient_dispense_detail_pivot.columns = [(s2) if s1 == 'dispence_id' else (s1) for s1,s2 in df_patient_dispense_detail_pivot.columns]
#next calculate the total quantity of medicines dispensed





#------------------------------------------------------------------------------------------------------------------------------
# creating the consumption details for the LAC bulk consumption
# storing the path for stock_consumption file, this file has the details about the bulk LAC consumption, relevant fields are 
# inventory_detail_id, consume_type, quantity, and loc_id. This needs to be merged with other datasets such as facility_sub_location,
#facility_location to get the information about the location. Along with that the details from inventory_details table about the 
#product_id 
path_stock_consumption = "C:/Users/dmandal/Desktop/LAC consumption  issue/stock_consumption_1_Dec.csv"
#importing dataset - loading the stock_consumption data
df_stock_consumption = pd.read_csv(path_stock_consumption,header = 0)
#Loading the facility location data
path_facility_location = "C:/Users/dmandal/Desktop/LAC consumption  issue/facility_location.csv"
#importing the dataset - loading the facility location data
df_facility_location = pd.read_csv(path_facility_location,header = 0)
# loading the LAC location data
path_facility_sub_location = "C:/Users/dmandal/Desktop/LAC consumption  issue/facility_sub_location.csv"
#importing the dataset - loading the facility sub location data
df_facility_sub_location = pd.read_csv(path_facility_sub_location,header = 0)
#inserting information such as state, SACS, ART Code, and district in the LAC file
df_facility_sub_location.insert(10,'Linked_State_name',df_facility_sub_location['fac_id'].map(facility_location.set_index('fac_id')['State_name']))
df_facility_sub_location.insert(11,'Linked_SACS',df_facility_sub_location['fac_id'].map(facility_location.set_index('fac_id')['SACS']))
df_facility_sub_location.insert(12,'Linked_ART_code',df_facility_sub_location['fac_id'].map(facility_location.set_index('fac_id')['art_code']))
df_facility_sub_location.insert(13,'Linked_District',df_facility_sub_location['fac_id'].map(facility_location.set_index('fac_id')['District']))

#inserting the ART_code, SACS, State in the stock_consumption file
df_stock_consumption.insert(3,'Linked_State_name',df_stock_consumption['loc_id'].map(df_facility_sub_location.set_index('sub_fac_id')['Linked_State_name']))
df_stock_consumption.insert(4,'Linked_SACS',df_stock_consumption['loc_id'].map(df_facility_sub_location.set_index('sub_fac_id')['Linked_SACS']))
df_stock_consumption.insert(5,'Linked_ART_code',df_stock_consumption['loc_id'].map(df_facility_sub_location.set_index('sub_fac_id')['Linked_ART_code']))
df_stock_consumption.insert(6,'Linked_District',df_stock_consumption['loc_id'].map(df_facility_sub_location.set_index('sub_fac_id')['Linked_District']))

#next pulling the product from the inventory details dataset
path_inventory_details = "C:/Users/dmandal/Desktop/LAC consumption  issue/inventory_details.csv"
#importing dataset - loading the inventory_details data
df_inventory_details = pd.read_csv(path_inventory_details,header = 0)
#adding the product_id from inventory_details dataset to the stock_consumption dataset
df_stock_consumption.insert(7,'product_id',df_stock_consumption['inventory_detail_id'].map(df_inventory_details.set_index('invent_detail_id')['product_id']))
#adding the product_id name from  product_master file
df_stock_consumption.insert(8,'product_name',df_stock_consumption['product_id'].map(product_master.set_index('product_id')['product_name']))
#the stock consumption table has different 'consume_type' with different code, code 6 is relevant as it refers to consumption,
#other codes are 1:Testing, 2:Control, 3: Quality Insurance, 4: Training, 5: Wastage, 6: Consumption, 7: Wastage, the first 4 apply to the lab components
df_stock_consumption_ARV= df_stock_consumption[df_stock_consumption['consume_type'] == 6]
#create a 2-D table with rows having State, district, facility and columns having the ARV drugs and their monthly consumption
#first step is grouping the LAC consumption data and having the pd.Grouper option
df_stock_consumption_ARV['created_date'] = pd.to_datetime(df_stock_consumption_ARV['created'], errors = 'coerce')

df_stock_consumption_ARV_group = df_stock_consumption_ARV.groupby(['Linked_State_name','Linked_SACS','Linked_District','Linked_ART_code',pd.Grouper(key = 'created_date',freq = 'Y')])['consume_id'].count().reset_index().sort_values('created_date')
df_stock_consumption_ARV_group['created_date'] = df_stock_consumption_ARV_group['created_date'].dt.date
#to be further refined to have sum of LAC dispensation as column name and date having no time
df_stock_consumption_ARV_group_pivot = pd.pivot_table(df_stock_consumption_ARV_group,index = ['Linked_State_name','Linked_SACS','Linked_District','Linked_ART_code'], columns = ['created_date'], fill_value =0, margins = True,margins_name='Total_dispensation').reset_index()
#total column is showing average please check also there is mismatch in number please check
df_stock_consumption_ARV_group_pivot.columns = [(s2) if s1 == 'consume_id' else (s1) for s1,s2 in df_stock_consumption_ARV_group_pivot.columns]
#the quantity is to be included in the dispensation











#--------------------------------------------------------------------------------------------------------------
#next creating the code for capturing the consumption of stock through stock adjustment
#for getting the adjusted stock (which is consumed), tables such as stock adjustment, inventory_details, and facility location
#relevant fields are loc_id, 'new_stock' (refers to pill adjusted),adjust_type 2 means write off (which is relevant),
#'adjust_reason' the relevant codes are dispensed (9), disp no entry (10), inventory_detail_id will give product_id
#stock the path of the stock_adjustment file
path_stock_adjustment = "C:/Users/dmandal/Desktop/LAC consumption  issue/stock_adjustment_1_Dec.csv"
#importing dataset - loading the stock_consumption data
df_stock_adjustment = pd.read_csv(path_stock_adjustment,header = 0)
#inserting the details from the loc_id, State, SACS, district, facility name and art_code
df_stock_adjustment.insert(3,'Linked_State_name',df_stock_adjustment['loc_id'].map(facility_location.set_index('fac_id')['State_name']))
df_stock_adjustment.insert(4,'Linked_SACS',df_stock_adjustment['loc_id'].map(facility_location.set_index('fac_id')['SACS']))
df_stock_adjustment.insert(5,'Linked_ART_code',df_stock_adjustment['loc_id'].map(facility_location.set_index('fac_id')['art_code']))
df_stock_adjustment.insert(6,'Linked_District',df_stock_adjustment['loc_id'].map(facility_location.set_index('fac_id')['District']))
#next selecting the relevant 'adjust_type' and 'adjust_reason', the adjust type should be 'write-off'(2), and adjust reason
#should be 'Dispensed' (9) and 'Disp no entry' (10)
df_stock_adjustment_ARV = df_stock_adjustment[(df_stock_adjustment['adjust_type'] == 2) & ((df_stock_adjustment['adjust_reason'] == 9) | (df_stock_adjustment['adjust_reason'] == 10))]









