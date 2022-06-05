import pandas as pd

from pickle import dump
from data_preprocessing.utils_data_handling import connect, load_select_pad


# Connection parameters
param_dic = {
    "host"      : "localhost",
    "database"  : "sensordata",
    "user"      : "postgres",
    "password"  : "JNt8hSW4l+MJ]p;.g[WG"
}

#Connect to the database
conn = connect(param_dic)

#Create dictionary to store which dataset belongs to which sensortype
sensor_dic = {
    "vibration_sensor" : {},
    "acceleration_sensor" : {},
    "velocity_sensor" : {},
    "position_sensor" : {},
    "temperature_sensor" : {},
    "gyroscope" : {},
    "voltage_sensor" : {},
    "current_sensor" : {},
    "power_sensor" : {}#,
    }



# ## CaseWesternReserve_BearingDataCenter

# ### 1. Dataset - Baseline (cwr_base4hp)
cwr_base4hp1730rpm = load_select_pad(conn, "cwr_base4hp1730rpm")

# ### 2. Dataset - Baseline (cwr_default0hp1797rpm)
cwr_default0hp1797rpm = load_select_pad(conn, "cwr_default0hp1797rpm")

# ### 3. Dataset - Baseline (cwr_fefault0hp1797rpm)
cwr_fefault0hp1797rpm = load_select_pad(conn, "cwr_fefault0hp1797rpm")

#create one large DataFrame
case_western_reserve = pd.concat([cwr_base4hp1730rpm,cwr_default0hp1797rpm,cwr_fefault0hp1797rpm],axis=1)
#add large DataFrame to dictionary
sensor_dic["vibration_sensor"]["case_western_reserve"] = case_western_reserve



# ## FemtoBearing DataSet
# 
# ### 1. Dataset - Vibration sensor (femto_vib_c1)
femto_vib_c1 = load_select_pad(conn, "femto_vib_c1")
femto_vib_c1 = femto_vib_c1.iloc[:,4:]
sensor_dic["vibration_sensor"]["femto"] = femto_vib_c1

# ### 2. Dataset - Temperature sensor (femto_temp_c1)
femto_temp_c1 = load_select_pad(conn, "femto_temp_c1")
femto_temp_c1 = femto_temp_c1.iloc[:,4:] #leave out time stamps
sensor_dic["temperature_sensor"]["femto"] = femto_temp_c1



# ## IMS Bearing DataSet
# 
# File Name: 2003.11.01.18.01
ims_bearing = load_select_pad(conn, "ims_bearing")
sensor_dic["vibration_sensor"]["ims_bearing"] = ims_bearing



# ## SensiML DataSet
#
# File Name: 08_12_chris_Balance_20190812T192339.csv
sensiml = load_select_pad(conn, "sensiml")
sensor_dic["vibration_sensor"]["sensiml"] = sensiml.iloc[:,:3]
sensor_dic["gyroscope"]["sensiml"] = sensiml.iloc[:,3:]



# ## CNC MillToolWear DataSet
# 
cnc_mill_tool_wear_exp1 = load_select_pad(conn, "cnc_mill_tool_wear")

#>>z-axis current feedback, dcbus voltage, output voltage, output current<<  are all zeroes throughout the entire dataset
#---------Build subsets of data--------------------
cnc_mill_tool_wear_exp1_acc = cnc_mill_tool_wear_exp1[["x1_actualacceleration","y1_actualacceleration","z1_actualacceleration","s1_actualacceleration"]]
cnc_mill_tool_wear_exp1_vel = cnc_mill_tool_wear_exp1[["x1_actualvelocity","y1_actualvelocity","z1_actualvelocity","s1_actualvelocity"]]
cnc_mill_tool_wear_exp1_pos = cnc_mill_tool_wear_exp1[["x1_actualposition","y1_actualposition","z1_actualposition","s1_actualposition"]]

cnc_mill_tool_wear_exp1_vlt = cnc_mill_tool_wear_exp1[["x1_dcbusvoltage","y1_dcbusvoltage","s1_dcbusvoltage","x1_outputvoltage","y1_outputvoltage","s1_outputvoltage"]]
cnc_mill_tool_wear_exp1_cur = cnc_mill_tool_wear_exp1[["x1_outputcurrent","y1_outputcurrent","s1_outputcurrent","x1_currentfeedback","y1_currentfeedback","s1_currentfeedback"]]
cnc_mill_tool_wear_exp1_pwr = cnc_mill_tool_wear_exp1[["x1_outputpower","y1_outputpower","s1_outputpower"]]

#--------Add to data dictionary----------------------
sensor_dic["acceleration_sensor"]["cnc_mill_tool_wear_exp1"] = cnc_mill_tool_wear_exp1_acc
sensor_dic["velocity_sensor"]["cnc_mill_tool_wear_exp1"] = cnc_mill_tool_wear_exp1_vel
sensor_dic["position_sensor"]["cnc_mill_tool_wear_exp1"] = cnc_mill_tool_wear_exp1_pos
sensor_dic["voltage_sensor"]["cnc_mill_tool_wear_exp1"] = cnc_mill_tool_wear_exp1_vlt
sensor_dic["current_sensor"]["cnc_mill_tool_wear_exp1"] = cnc_mill_tool_wear_exp1_cur
sensor_dic["power_sensor"]["cnc_mill_tool_wear_exp1"] = cnc_mill_tool_wear_exp1_pwr



# ## MillingNASA
# 
millingnasa = load_select_pad(conn, "millingnasa")
sensor_dic["vibration_sensor"]["millingnasa"] = millingnasa[["vib_table","vib_spindle"]]
sensor_dic["current_sensor"]["millingnasa"] = millingnasa[["smcac","smcdc"]]



# ## One Year Industrial Component Degradation
# 
OYICD = load_select_pad(conn, "industrial_component_degradation")
sensor_dic["position_sensor"]["OYICD"] = OYICD[["pcut__ctrl_position_controller__actual_position","psvolfilm__ctrl_position_controller__actual_position"]]
sensor_dic["velocity_sensor"]["OYICD"] = OYICD[["pcut__ctrl_position_controller__actual_speed","psvolfilm__ctrl_position_controller__actual_speed"]]



# ## Condition monitoring hydraulic systems
# 
#---------Loading and selecting data-----------------
cond_mon_ts1_df = load_select_pad(conn, "cond_mon_ts1_df")
cond_mon_ts2_df = load_select_pad(conn, "cond_mon_ts2_df")
cond_mon_ts3_df = load_select_pad(conn, "cond_mon_ts3_df")
cond_mon_ts4_df = load_select_pad(conn, "cond_mon_ts4_df")
cond_mon_fs1_df = load_select_pad(conn, "cond_mon_fs1_df")
cond_mon_fs2_df = load_select_pad(conn, "cond_mon_fs2_df")
cond_mon_eps1_df = load_select_pad(conn, "cond_mon_eps1_df")
cond_mon_vs1_df = load_select_pad(conn, "cond_mon_vs1_df")

#--------Concatenate data of same devices-----------
cond_mon_ts = pd.concat([cond_mon_ts1_df["cycle_0"],cond_mon_ts2_df["cycle_0"],cond_mon_ts3_df["cycle_0"],cond_mon_ts4_df["cycle_0"]], axis=1)
cond_mon_ts.columns = ["ts1","ts2","ts3","ts4"]
#cond_mon_fs = pd.concat([cond_mon_fs1_df["cycle_0"],cond_mon_fs2_df["cycle_0"]], axis=1)
#cond_mon_fs.columns = ["fs1","fs2"]

#--------Add to data dictionary----------------------
sensor_dic["temperature_sensor"]["cond_mon"] = cond_mon_ts
#sensor_dic["volume_flow_sensor"]["cond_mon"] = cond_mon_fs
sensor_dic["power_sensor"]["cond_mon_EPS1"] = cond_mon_eps1_df["cycle_0"]
sensor_dic["vibration_sensor"]["cond_mon_VS1"] = cond_mon_vs1_df["cycle_0"]



# ## Genesis demonstrator data
# 
genesis_demonstrator = load_select_pad(conn, "genesis_demonstrator")
sensor_dic["acceleration_sensor"]["genesis_demonstrator"] = genesis_demonstrator["motordata_isacceleration"]
sensor_dic["velocity_sensor"]["genesis_demonstrator"] = genesis_demonstrator["motordata_actspeed"]
sensor_dic["current_sensor"]["genesis_demonstrator"] = genesis_demonstrator["motordata_actcurrent"]



# ## High Storage System Data
# 
high_storage_system = load_select_pad(conn, "high_storage_system")

high_storage_system_pos = high_storage_system[["i_w_blo_weg","i_w_bhl_weg","i_w_bhr_weg","i_w_bru_weg","i_w_hr_weg","i_w_hl_weg"]]
high_storage_system_vol = high_storage_system[["o_w_blo_voltage","o_w_bhl_voltage","o_w_bhr_voltage","o_w_bru_voltage","o_w_hr_voltage","o_w_hl_voltage"]]
high_storage_system_pwr = high_storage_system[["o_w_blo_power","o_w_bhl_power","o_w_bhr_power","o_w_bru_power","o_w_hr_power","o_w_hl_power"]]

#--------Add to data dictionary----------------------
sensor_dic["position_sensor"]["high_storage_system"] = high_storage_system_pos
sensor_dic["voltage_sensor"]["high_storage_system"] = high_storage_system_vol
sensor_dic["power_sensor"]["high_storage_system"] = high_storage_system_pwr



# ## POT Daten
# 
pot_data = load_select_pad(conn, "pot_data")

#Acceleration sensors contain all zeroes
pot_data_vel = pot_data[["axes_axis_5___be___phi_actvelo","axes_axis_9___be___rs2o_actvelo","axes_axis_6___be___y_actvelo"]]
pot_data_pos = pot_data[["axes_axis_5___be___phi_actpos","axes_axis_9___be___rs2o_actpos","axes_axis_6___be___y_actpos"]]

#--------Add to data dictionary----------------------
sensor_dic["velocity_sensor"]["pot_data"] = pot_data_vel
sensor_dic["position_sensor"]["pot_data"] = pot_data_pos



# ## HMI Demonstrator
# 
hmi_demonstrator = load_select_pad(conn, "hmi_demonstrator")
sensor_dic["position_sensor"]["hmi_demonstrator"] = hmi_demonstrator["actpos"]


#write dictionary to simplify loading for every experiment 
with open(r'data_preprocessing/sensor_dic.pkl', 'wb') as filehandle:
    dump(sensor_dic, filehandle)
    
#with open(r'data_preprocessing/sensor_dic_normalized.pkl', 'wb') as filehandle:
#    dump(sensor_dic, filehandle)

