#this code is to get the breakup of patient status wise across different facilities from the IMS data. This is done using the patient_master
#dataset but the drawback is that it derives from the latest dataset only (for status) i.e. historical value cannot be claculated For historical value
#dataset such as 'patient_status_change' may be used. This code can also be used to get the break up of beneficiaries age wise/risk factor wise
#across different facilities
import pandas as pd
import datetime
from datetime import datetime
import numpy as np
import os
#changing the working directory
os.chdir("C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/5.Module/CST/Other docs/1.Patient Master/Processed files")
#selecting the number of columns to be visible for the data frame
pd.options.display.max_columns =30
#storing the path of the patient_master file,will vary across system, created on 31st March dataset, replace with latest dataset
path_patient_master = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/1.Patient Master/patient_master_31 march.csv"
#IMPORTING DATASET - reading the patient_master dataset
df_patient_master=pd.read_csv(path_patient_master,header = 0 )
#naming the columns (total 57 columns), in case the file does not have column names
#df_patient_master.columns = [" patient_id","fac_id","patient_type","patient_code","barcode","patient_name","gender","birth_date","category","identity_val_1","identity_val_2","identity_val_3","identity_val_4","art_eligible_date","pre_art_num","art_num","register_date","first_dispense_date","last_dispense_date","next_appoint_date","regimen","patient_status","last_status_change_on","linked_out","link_art_id","family_mem","family_id","pregnent","cd4_bas eline","cd4_baseline_date","last_cd4_count","last_cd4_date","next_cd4_date","viral_baselin e","viral_baseline_date","last_viral_count","last_viral_date","next_viral_date","address","c ity","district","state","pin_code","mobile","weight_band","entry_point","risk_factor_for_hiv","e ducation","monthly_household_income","occupation","is_transfer","transfer_to","status","crea ted_by","created","modified","modified_by "]
#columns_to_be_removed = ["ba rcode",'patient_name','identity_val_1', 'identity_val_2','iden tity_val_3',"identity_val_4",'address','c ity','district','state','pin_code','mobile','e ducation','monthly_household_income','occupation','crea ted_by','created']
#df_patient_master.drop(columns_to_be_removed, axis=1, inplace = True)
#removing rows having ART text as 'deleted' since they are deleted records
df_patient_master = df_patient_master[~(df_patient_master.art_num.str.contains('deleted', na=False))]
#removing rows based on patient_type with values 'T' and 'P', These refer to transit and PEP dispensation, they donot have any status or pre-ART num
df_patient_master = df_patient_master[~(df_patient_master['patient_type'].isin(['T','P']))]
#importing location file, this file has IMS location data mapped to the MPR ART Code
path_location = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/4.Facility Master/4April2019_Location_data.xlsx"
facility_location = pd.read_excel(path_location,header = 0)
#changing date variables from object to datetime and numeric
df_patient_master[["art_eligible_date","register_date", "first_dispense_date","last_dispense_date","next_appoint_date","cd4_baseline_date","last_cd4_date","next_cd4_date","viral_baseline_date","last_viral_date","next_viral_date","birth_date"]] = df_patient_master[["art_eligible_date","register_date", "first_dispense_date","last_dispense_date","next_appoint_date","cd4_baseline_date","last_cd4_date","next_cd4_date","viral_baseline_date","last_viral_date","next_viral_date","birth_date"]].apply(pd.to_datetime, errors ='coerce')
df_patient_master[["cd4_baseline", "last_cd4_count","viral_baseline","last_viral_count","patient_status"]] = df_patient_master[["cd4_baseline", "last_cd4_count","viral_baseline","last_viral_count","patient_status"]].apply(pd.to_numeric)
#mapping patient_status to new column patient_status_variable, apart from 1 to 12
patient_status_mapping = {1:"Pre-ART Alive", 2:"On-ART Alive",3:"Pre-ART LFU", 4:"Pre-ART Died",5:"Pre-ART Opted Out",6:"Eligible for ART",7:"Pre-ART MIS",8:"ART Died",9:"ART LFU",10:"ART Opted Out",11:"ART Stopped",12:"ART MIS", not(1,2,3,4,5,6,7,8,9,10,11,12): "Status could not be mapped", None: "Status could not be mapped"}
df_patient_master["patient_status_variable"] = df_patient_master['patient_status'].map(patient_status_mapping)
#mapping regimen, this can be derived from the file regimen master, this file sometimes gets updated, so check if new regimen to be added to the list
regimen_mapping = {1:"TLE Adult- RG1", 2:"ZLN Adult- RG2",3:"ZLN Ped- RG3", 4:"ZL+ E Ped- RG4",5:"TL+N Adult- RG5",7:"ZL+E Adult- RG7",8:"AL+EFV Ped- RG8",15:"AL+EFV Adult- RG15",16:"AL+NVP Adult- RG16",17:"TL+ATV/r Adult- RG17",18:"ZL+ATV/r Adult- RG18",21:"AL+ATV/r Adult- RG21",22:"TL+LPV/r Adult- RG22",23:"ZL+LPV/r Adult- RG23",25:"AL+LPV/r Adult- RG25",28:"ZL+LPV/r Ped- RG28",30:"AL+LPV/r Ped- RG30",31:"AL+ LPV/r-Syp Ped- RG31",32:"AL (P) + NVP (A) Ped- RG32",33:"ZL (A) + EFV (P) Ped- RG33",34:"ZL (P) + LPV/r- SYP PED- RG34",35:"AL (P) + NVP (P) PED- RG35",38:"AL (A) + EFV (P) PED- RG38",39:"AL(PED)+EFV(Adult)",41:"Adult - SL+ATV/r",42:"SLN (A)",43:"AL(Adult)",44:"Lopinavir/Ritonavir Oral Pellets(40/10mg)",not(1,2,3,4,5,7,8,15,16,17,18,21,22,23,25,28,30,31,32,33,34,35,38,39,41,42,43,44): "Regimen could not be mapped", None: "Status could not be mapped"}
df_patient_master["regimen_variable"] = df_patient_master['regimen'].map(regimen_mapping)
#mapping the drug line (1st Line, 2nd Line) to the regimen
regimen1_mapping = {1:"1st Line", 2:"1st Line",3:"1st Line", 4:"1st Line",5:"1st Line",7:"1st Line",8:"1st Line",15:"1st Line",16:"1st Line",17:"2nd Line",18:"2nd Line",21:"2nd Line",22:"2nd Line",23:"2nd Line",25:"2nd Line",28:"1st Line",30:"2nd Line",31:"1st Line",32:"1st Line",33:"1st Line",34:"1st Line",35:"1st Line",38:"1st Line",39:"1st Line",41:"2nd Line",42:"1st Line",43:"1st Line",44:"2nd Line",not(1,2,3,4,5,7,8,15,16,17,18,21,22,23,25,28,30,31,32,33,34,35,38,39,41,42,43,44): "Regimen could not be mapped", None: "Status could not be mapped"}
df_patient_master["regimen_drug_line_variable"] = df_patient_master['regimen'].map(regimen1_mapping)
#mapping risk factor 'risk_factor_for_hiv' to new column risk_factor_for_hiv_variable
risk_factor_for_hiv_mapping = {21:"Heterosexual", 22:"MSM",23:"Injecting Drug Use", 24:"Blood transfusion",25:"Mother to child",26:"Probable unsafe injection",27:"Unknown",28:"Commercial Sex Worker",29:"Migrant",30:"Trucker",not(21,22,23,24,25,26,27,28,29,30): "Risk Factor could not be mapped", None: "Status could not be mapped"}
df_patient_master["risk_factor_for_hiv_variable"] = df_patient_master['risk_factor_for_hiv'].map(risk_factor_for_hiv_mapping)
#getting the age column, calculated from birth date, target date is the date of the extraction of dataset
df_patient_master['target_date'] = pd.to_datetime('2019-03-31')
df_patient_master['age-years']=round((((df_patient_master['target_date']-df_patient_master['birth_date']).astype('timedelta64[D]'))/365.2425),1)
#getting age range for the beneficiaries, please note that the age group can vary
bins_all = [1,3,6,10,12,14,16,18,20,25,30,35,40,45,50,55,110]
labels_all = ['1-2','3-5','6-9','10-11','12-13','14-15','16-17','18-19','20-24','25-29','30-34','35-39','40-44','45-49','50-54','55+']
df_patient_master['all_age_range'] = pd.cut(df_patient_master['age-years'],bins_all, labels = labels_all,include_lowest = True, right = False)
# mapping state,facility name and ART code into the file
df_patient_master.insert(2,'State',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['State']))
df_patient_master.insert(3,'Facility_Name',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['Facility Name']))
df_patient_master.insert(4,'MPR_ART_Code',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['MPR Code']))
df_patient_master.insert(5,'SACS',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['Name of SACS']))
# exporting the data to a csv file
date_in_file_name = datetime.today().strftime('%Y-%m-%d')
df_patient_master.to_csv(date_in_file_name + " df_patient_master_extra_columns.csv")
# creating a table having status of different beneficiaries (this include Linked out beneficiary)
df_patient_status_facilitywise_table_all = pd.pivot_table(df_patient_master, values='patient_id', index=['State','fac_id','MPR_ART_Code','Facility_Name'],columns=['patient_status_variable'], aggfunc=pd.Series.nunique,fill_value = '', margins = True, margins_name = "Total_count").reset_index()
#creating patient master without linked out patient
df_patient_master_without_linked_out = df_patient_master[~(df_patient_master['linked_out'].isin(['Y']))]
# creating a table having status of different beneficiaries (this excludes Linked out beneficiary)
df_patient_status_facilitywise_table_without_linked_out = pd.pivot_table(df_patient_master_without_linked_out, values='patient_id', index=['State','fac_id','MPR_ART_Code','Facility_Name'],columns=['patient_status_variable'], aggfunc=pd.Series.nunique,fill_value = '', margins = True, margins_name = "Total_count").reset_index()
#writing the file to excel format
writer = pd.ExcelWriter(date_in_file_name + ' Facility_patient_status_analysis.xlsx', engine='xlsxwriter')
df_patient_status_facilitywise_table_all.to_excel(writer, sheet_name = 'All_patient_including_LAC', startrow=3,startcol=1)
df_patient_status_facilitywise_table_without_linked_out.to_excel(writer, sheet_name = 'Patients_without_LAC', startrow=3,startcol=1)
#adding a header and format to the sheets
workbook = writer.book
worksheet1 = writer.sheets['All_patient_including_LAC']
worksheet2 = writer.sheets['Patients_without_LAC']
header_format = workbook.add_format({'bold': True,'text_wrap': False,'valign': 'top','fg_color': '#D7E4BC','border': 1})
worksheet1.write(1,3,"Beneficiary count across different facilities (including linked out patients)",header_format)
worksheet2.write(1,3,"Beneficiary count across different facilities (excluding linked out patients)",header_format)
                                     
writer.save()
#---------------------------------------------------------------------------------------
#but the above was on 31 march data set, if it is to be calculated for a particular month use patient_status_change file
#the code of the patient status month wise is below
import pandas as pd
import datetime
from datetime import datetime
import numpy as np
import os
#changing the working directory
os.chdir("C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/1.Patient Master/Processed files")
#selecting the number of columns to be visible for the data frame
pd.options.display.max_columns =30

#storing the path of the patient_status_change,will vary across system, created on 7th March dataset, replace with latest dataset
path_status_change = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/2.Patient Status change/patient_status_change_7 march.csv"
#IMPORTING DATASET - reading the patient_master dataset
df_status_change=pd.read_csv(path_status_change,header = 0 )
#changing to datetime
df_status_change[["datetime"]] = df_status_change[["datetime"]].apply(pd.to_datetime)
df_status_change['datetime_m_y']=df_status_change.datetime.map(lambda x: x.strftime('%Y-%m'))
df_status_change=df_status_change.sort_values(by='datetime')
#removing the duplicates, for one patient keeping the last status (of the month)
df_status_change1 = df_status_change.drop_duplicates(subset=['patient_id','datetime_m_y'],keep='last')
#dropping columns such as change_id, change_by
df_status_change1.drop(["change_id","change_by","datetime","pre_status"], axis=1, inplace = True)
#building a table with patient id in row and column with dates, the cell value status of the patient, table showing patient id and status across diferent month
#testing on a smaller data set, this is working, the only issue is that there are certain gaps in dates (where stauts is not present)
#df_status_change2 = df_status_change1[df_status_change1['datetime_m_y'] > '2018-06']
#df_patient_status_change_table_2 = pd.pivot_table(df_status_change2, index=['patient_id'],columns=['datetime_m_y']).reset_index()
#converting multi index column to single one
#df_patient_status_change_table_2.columns = [c[0] + c[1] for c in df_patient_status_change_table_2.columns]
#storing columns where the forward fill to be adopted
#col_df_patient_status_change_table_2 = (list(df_patient_status_change_table_2.columns))
#col_df_patient_status_change_table_2.remove("patient_id")
#forward fill to complete the datafrane, filling the data from  column to subsequent column
#df_patient_status_change_table_2[col_df_patient_status_change_table_2] = df_patient_status_change_table_2[col_df_patient_status_change_table_2].ffill(axis = 1)
#for complete data set creating the status change table, no. of patients will be less than 2.7Mn since some patients have not changed the status
df_patient_status_change_table = pd.pivot_table(df_status_change1, index=['patient_id'],columns=['datetime_m_y']).reset_index()
#converting multi index column to single one
df_patient_status_change_table.columns = [c[0] + c[1] for c in df_patient_status_change_table.columns]
#storing columns where the forward fill to be adopted
col_df_patient_status_change_table = (list(df_patient_status_change_table.columns))
col_df_patient_status_change_table.remove("patient_id")
#forward fill to complete the datafrane, filling the data from  column to subsequent column
df_patient_status_change_table[col_df_patient_status_change_table] = df_patient_status_change_table[col_df_patient_status_change_table].ffill(axis = 1)
#next step is to add art initiation date to the status change table so as to buiuld the complete cohort, also add age group, and HRG
#storing the path of the patient_master file,will vary across system, created on 31st March dataset, replace with latest dataset
path_patient_master = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/1.Patient Master/patient_master_31 march.csv"
#IMPORTING DATASET - reading the patient_master dataset
df_patient_master=pd.read_csv(path_patient_master,header = 0 )
#removing rows having ART text as 'deleted' since they are deleted records
df_patient_master = df_patient_master[~(df_patient_master.art_num.str.contains('deleted', na=False))]
#removing rows based on patient_type with values 'T' and 'P', These refer to transit and PEP dispensation, they donot have any status or pre-ART num
df_patient_master = df_patient_master[~(df_patient_master['patient_type'].isin(['T','P']))]
#importing location file, this file has IMS location data mapped to the MPR ART Code
path_location = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/4.Facility Master/4April2019_Location_data.xlsx"
facility_location = pd.read_excel(path_location,header = 0)
#mapping risk factor 'risk_factor_for_hiv' to new column risk_factor_for_hiv_variable
risk_factor_for_hiv_mapping = {21:"Heterosexual", 22:"MSM",23:"Injecting Drug Use", 24:"Blood transfusion",25:"Mother to child",26:"Probable unsafe injection",27:"Unknown",28:"Commercial Sex Worker",29:"Migrant",30:"Trucker",not(21,22,23,24,25,26,27,28,29,30): "Risk Factor could not be mapped", None: "Status could not be mapped"}
df_patient_master["risk_factor_for_hiv_variable"] = df_patient_master['risk_factor_for_hiv'].map(risk_factor_for_hiv_mapping)
#changing date variables from object to datetime and numeric
df_patient_master[["first_dispense_date","birth_date"]] = df_patient_master[["first_dispense_date","birth_date"]].apply(pd.to_datetime, errors ='coerce')
#getting the age column, calculated from birth date, target date is the date of the extraction of dataset
df_patient_master['target_date'] = pd.to_datetime('2019-03-31')
df_patient_master['age-years']=round((((df_patient_master['target_date']-df_patient_master['birth_date']).astype('timedelta64[D]'))/365.2425),1)
#getting age range for the beneficiaries, please note that the age group can vary
bins_all = [1,3,6,10,12,14,16,18,20,25,30,35,40,45,50,55,110]
labels_all = ['1-2','3-5','6-9','10-11','12-13','14-15','16-17','18-19','20-24','25-29','30-34','35-39','40-44','45-49','50-54','55+']
df_patient_master['all_age_range'] = pd.cut(df_patient_master['age-years'],bins_all, labels = labels_all,include_lowest = True, right = False)
# mapping state,facility name and ART code into the file
df_patient_master.insert(2,'State',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['State']))
df_patient_master.insert(3,'Facility_Name',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['Facility Name']))
df_patient_master.insert(4,'MPR_ART_Code',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['MPR Code']))
df_patient_master.insert(5,'SACS',df_patient_master['fac_id'].map(facility_location.set_index('Facility ID')['Name of SACS']))
# inserting columns such as risk factor, age-years,gender, state, facility name, MPR_ART_Code, and SACS to df_patient_status_change_table
df_patient_status_change_table.insert(2,'first_dispense',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['first_dispense_date']))
df_patient_status_change_table.insert(3,'Gender',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['gender']))
df_patient_status_change_table.insert(4,'Risk_factor',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['risk_factor_for_hiv_variable']))
df_patient_status_change_table.insert(5,'Age',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['age-years']))
df_patient_status_change_table.insert(6,'State',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['State']))
df_patient_status_change_table.insert(7,'MPR_ART_Code',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['MPR_ART_Code']))
df_patient_status_change_table.insert(8,'SACS',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['SACS']))
df_patient_status_change_table.insert(9,'fac_id',df_patient_status_change_table['patient_id'].map(df_patient_master.set_index('patient_id')['fac_id']))
#next set of codes would be to create the respective codes, initiation day, segment, and count of patients of particular status
#create two tables using group by and then divide them to get retention percentage
df_patient_status_change_table['first_dispense'] = df_patient_status_change_table['first_dispense'].apply(pd.to_datetime, errors ='coerce')
# adding month and year for the first dispense
df_patient_status_change_table['first_dispense_m_y']=df_patient_status_change_table.first_dispense.apply(lambda x: x.strftime('%Y-%m') if not pd.isnull(x) else '')
#groupby different columns, #count of the beneficiary initiating dispensation in that month 
retention_table = df_patient_status_change_table.groupby(['first_dispense_m_y']).size().reset_index()
retention_table.columns = ['Initiation_period','no_of_beneficiaries']
#select the status, it will run for all 12 statuses
status = 2
retention_table_2=pd.merge(retention_table,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2019-03'] == status).agg('sum')).reset_index(name='count_new_status_2_2019-03'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2019-02'] == status).agg('sum')).reset_index(name='count_new_status_2_2019-02'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2019-01'] == status).agg('sum')).reset_index(name='count_new_status_2_2019-01'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-12'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-12'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-11'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-11'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-10'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-10'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-09'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-09'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-08'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-08'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-07'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-07'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-06'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-06'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-05'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-05'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-04'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-04'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-03'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-03'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-02'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-02'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2018-01'] == status).agg('sum')).reset_index(name='count_new_status_2_2018-01'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-12'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-12'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-11'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-11'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-10'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-10'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-09'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-09'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-08'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-08'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-07'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-07'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-06'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-06'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-05'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-05'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-04'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-04'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-03'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-03'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-02'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-02'))
retention_table_2=pd.merge(retention_table_2,df_patient_status_change_table.groupby(['first_dispense_m_y']).apply(lambda x: (x['new_status2017-01'] == status).agg('sum')).reset_index(name='count_new_status_2_2017-01'))
#the data set patient_status_change has certain inconsistencies, (since the status were changed in Jan 2019), hence in order to get complete cohort the dataset, patient_monthly_status is to be used
path_monthly_status = "C:/Users/dmandal/Desktop/CHAI Documents/SOCH Documents/2.CST Module/CST/Other docs/2.Patient Status change/patient_monthly_status_7 march.csv"
#importing monthly status change dataset
df_monthly_status_change=pd.read_csv(path_monthly_status,header = 0 )
#changing to datetime
df_monthly_status_change[["status_date"]] = df_monthly_status_change[["status_date"]].apply(pd.to_datetime)
df_monthly_status_change['status_date_m_y']=df_monthly_status_change.status_date.map(lambda x: x.strftime('%Y-%m'))

