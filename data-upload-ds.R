#################################################################
## Purpose: data munging and import into the desert springs SQL database
##          Use after running agol-download-ds.R and utils.R
## Date: 09/09/2020
## R version written with: 4.0.2
## Libraries and versions used: libraries specified in Main.R.
## Calls python script"download-photos-ArcPro.py" 
##      - make sure you can import arcpy before running!
################################################################

#---------Variables used----------# uncomment either the testing set or the real set 
# set this to the location of the downloaded FGDB from AGOL

# filepaths for testing import:
gdb.path <- "C:\\Users\\EEdson\\Desktop\\MOJN\\DS_Springs_LizzyTest.gdb"
photo.dest <- "C:\\Users\\EEdson\\Desktop\\MOJN\\Photos_DS"
originals.dest <- "C:\\Users\\EEdson\\Desktop\\MOJN\\Photo_Originals"
db.params.path <- "C:\\Users\\EEdson\\Desktop\\MOJN\\mojn-ds-datatransfer\\ds-database-conn.csv"

# filepaths for the real import:
# gdb.path <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\FieldData\\Lakes_Annual\\STLK_AnnualLakeVisit_20191022.gdb"
# photo.dest <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\ImageData\\Lakes"
# originals.dest <- "M:\\MONITORING\\_FieldPhotoOriginals_DoNotModify\\AGOL_STLK"
# db.params.path <- "C:\\Users\\EEdson\\Desktop\\MOJN\\mojn-ds-datatransfer\\ds-database-conn.csv"
#---------------------------#


#------------ Lookup tables form SQL database used--------------#
## Get Site table from SQL database
params <- readr::read_csv(db.params.path) %>% 
  as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)
sites <- dplyr::tbl(conn, dbplyr::in_schema("data", "Site")) %>%
  dplyr::collect()

# Get lookups of all photo codes for photo naming. 
#These are used for naming the internal photos and getting the photo description code for import inot data.photos
# photo description look up variable (additional photos)
photo.types <- dplyr::tbl(conn, dbplyr::in_schema("ref", "PhotoDescriptionCode")) %>%
  dplyr::collect() %>%
  select(ID, Code)
# # new one that conbines photo SOP. incase we need this
# photo.types1 <- dplyr::tbl(conn, dbplyr::in_schema("ref", "PhotoDescriptionCode")) %>%
#   dplyr::collect() %>%
#   select(ID, Code, PhotoSOPID)
# 
# photo.types2 <-dplyr::tbl(conn, dbplyr::in_schema("lookup", "photoSOP")) %>%
#   dplyr::collect() %>%
#   select(ID, Code)
# names(photo.types2) <- c("PhotoSOPID", "PhotoSOP_Code")
# 
# photoTypes <- photo.types1 %>% 
#   left_join(photo.types2, by = "PhotoSOPID")

## Lifeform look up table ( for riparian vegetation)
lifeform.types <- dplyr::tbl(conn, dbplyr::in_schema("lookup", "Lifeform")) %>%
  dplyr::collect() %>%
  select(ID, Label, Code)

## Taxon table (for invasive species)
taxon.types <- dplyr::tbl(conn, dbplyr::in_schema("ref", "Taxon")) %>%
  dplyr::collect() %>%
  select(ID, USDAPlantsCode)

## Repeat photo look up ( for repeat photos)
repeat.types <- dplyr::tbl(conn, dbplyr::in_schema("lookup", "RepeatPhotoType")) %>%
  dplyr::collect() %>%
  select(ID, Code)
#-----------------------------------------------------------------------------------#


######################################################################################
## Download photos using Python script
visit.data <- paste(gdb.path, "MOJN_DS_SpringVisit", sep = "\\")
py.ver <- py_config()

# nif (!(py.ver$version < 3)) {
#   use_python("C:\\Python27\\ArcGISx6410.5", required = TRUE)
# }

source_python("download-photos-ds_photodict.py")

## Create Python dictionaries from the look up fields
photo.type.dict <- py_dict(photo.types$ID, photo.types$Code)
lifeform.type.dict <- py_dict(lifeform.types$ID, lifeform.types$Code)
taxon.type.dict <- py_dict(taxon.types$ID, taxon.types$USDAPlantsCode)
repeat.type.dict <- py_dict( repeat.types$ID, repeat.types$Code)


## Download photos from internal camera table this is the  riparian veg internal camera - Works!
if(nrow(riparianVegInt) == 0){
  print("There are no Riparian Veg internal camera images")
}else{
  photo.type.field <- "LifeForm"
  photo.table <- paste(gdb.path, "InternalCamera__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "InternalCamera", sep = "\\")
  repeat.data <- paste(gdb.path, "VegImageRepeat", sep = "\\")
  RV_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, repeatFeatureClass = repeat.data,
                                                 visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                                 originalsLocation = originals.dest, photoCodeDict = lifeform.type.dict , photoTypeField = photo.type.field)
  RV_IntImage <- as_tibble(RV_IntImage)
  RV_IntImage$VisitGUID <- str_remove_all(RV_IntImage$VisitGUID, "\\{|\\}")
  RV_IntImage$GlobalID <- str_remove_all(RV_IntImage$GlobalID, "\\{|\\}")
  RV_IntImage$RepeatGUID <- str_remove_all(RV_IntImage$RepeatGUID, "\\{|\\}")
}
    

## Download photos from Inv Image repeat table, these are the invasive internal camera images - works!
if(nrow(invasivesInt) == 0){
  print("There are no invasive internal camera images")
}else{
  photo.type.field <- "InvasiveSpecies"
  photo.table <- paste(gdb.path, "InvImageRepeat__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "InvImageRepeat", sep = "\\")
  repeat.data <- paste(gdb.path, "InvasivePlants", sep = "\\")
  Inv_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data,  repeatFeatureClass = repeat.data,
                                                 visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                                 originalsLocation = originals.dest, photoCodeDict = taxon.type.dict, photoTypeField = photo.type.field)
  Inv_IntImage <- as_tibble(Inv_IntImage)
  Inv_IntImage$VisitGUID <- str_remove_all(Inv_IntImage$VisitGUID, "\\{|\\}")
  Inv_IntImage$GlobalID <- str_remove_all(Inv_IntImage$GlobalID, "\\{|\\}")
  Inv_IntImage$RepeatGUID <- str_remove_all(Inv_IntImage$RepeatGUID, "\\{|\\}")
}


## Download photos from repeat photos internal table- works!
if(nrow(repeatsInt) == 0){
  print("There are no repeat photos internal images")
}else{
  photo.type.field <- "PhotoType"
  photo.table <- paste(gdb.path, "RepeatPhotos_Internal__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "RepeatPhotos_Internal", sep = "\\")
  repeat.data <- paste(gdb.path, "Repeats", sep = "\\")
  Rep_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, repeatFeatureClass = repeat.data,
                                                 visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                                 originalsLocation = originals.dest, photoCodeDict = repeat.type.dict, photoTypeField = photo.type.field)
  Rep_IntImage  <- as_tibble(Rep_IntImage )
  Rep_IntImage $VisitGUID <- str_remove_all(Rep_IntImage $VisitGUID, "\\{|\\}")
  Rep_IntImage $GlobalID <- str_remove_all(Rep_IntImage $GlobalID, "\\{|\\}")
  Rep_IntImage$RepeatGUID <- str_remove_all(Rep_IntImage$RepeatGUID, "\\{|\\}")
}

## Download photos from Additional Photo Internal table - works!
if(nrow(additionalPhotosInt) == 0){
  print("There are no additional internal photo images")
}else{
  photo.type.field <- "AdditionalPhotoType"
  photo.table <- paste(gdb.path, "AdditionalPhotoInternal__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "AdditionalPhotoInternal", sep = "\\")
  repeat.data <- paste(gdb.path, "AdditionalPhotos2", sep = "\\")
  AP_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, repeatFeatureClass = repeat.data,
                                       visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                       originalsLocation = originals.dest, photoCodeDict = photo.type.dict, photoTypeField = photo.type.field)
  AP_IntImage <- as_tibble(AP_IntImage)
  AP_IntImage$VisitGUID <- str_remove_all(AP_IntImage$VisitGUID, "\\{|\\}")
  AP_IntImage$GlobalID <- str_remove_all(AP_IntImage$GlobalID, "\\{|\\}")
  AP_IntImage$RepeatGUID <- str_remove_all(AP_IntImage$RepeatGUID, "\\{|\\}")
}

##################################################################################################

## upload the table data to SQL server
# create empty list for storing the tables
db <- list()

## Visit table -works!
db$Visit <- visit %>%
  select(SiteID,
         StartDateTime,
         Notes = SpringComments,
         GlobalID = globalid,
         SpringTypeID = SpringType,
         VisitTypeID = VisitType,
         MonitoringStatusID = Status) %>%
  mutate(VisitDate = format.Date(StartDateTime, "%Y-%m-%d"),
         StartTime = format.Date(StartDateTime, "%H:%M:%S"),
         DataProcessingLevelID = 1,  # Raw
         ProtocolID = 12 # first published version. TODO add this field to Survey123
         ) %>%
  left_join(select(sites, ID, ProtectedStatusID), by = c("SiteID" = "ID")) %>%
  select(-StartDateTime) 

visit.keys <- uploadData(db$Visit, "data.Visit", conn, keep.guid = FALSE)  # Insert into Visit table in database



## Visit Personnel table -Works!
db$VisitPersonnel <- observers %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         PersonnelID = FieldCrew) %>%
  mutate(PersonnelRoleID = 5)  # Field crew
personnel.keys <- uploadData(db$VisitPersonnel, "data.VisitPersonnel", conn, keep.guid = FALSE, cols.key = list(VisitID = integer(), PersonnelID = integer(), PersonnelRoleID = integer()))

## sensor deployment table - works!

db$SensorDeploy <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  filter(SensorDeployed == "Y") %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         SensorID = SensorIDDep,
         DeploymentTime,
         Notes = SensorDeployNote) %>% 
  mutate(DeploymentTimeOfDay= paste(DeploymentTime, ":00", sep = "")) %>% 
  select(-DeploymentTime)

sensorDep.keys <-uploadData(db$SensorDeploy, "data.SensorDeployment", conn, keep.guid = FALSE)

# ## sensor retrieval table - Hold on this, Sarah says SensorID ret is not deployment Sensor
# db$SensorRetrieve <- sensor %>% 
#   inner_join(visit.keys, by = c("parentglobalid" = "GlobalID")) %>% 
#   inner_join(visit, by = c("parentglobalid" = "globalid")) %>% 
#   select(IsSensorretrievedID = SensorRetrieved,
#          GlobalID = globalid,
#          SensorDeploymentID = SensorIDRet,
#          SensorProblemID = SensorProblem,
#          RetrievalTime,
#          DownloadSuccessful,
#          notes = SensorRetrieveNotes) %>% 
#   mutate(IsDownloadSuccessfulID = case_when( DownloadSuccessful == "Y" ~ 1,
#                                              DownloadSuccessful =="N" ~ 2,
#                                              DownloadSuccessful == "NA" ~ 8,
#                                              DownloadSuccessful == "ND" ~ 9),
#          RetrievalTimeOfDay= paste(RetrievalTime, ":00", sep = "")) %>% 
#   select(-RetrievalTime, -DownloadSuccessful)
# 
# sensorRet.keys <-uploadData(db$SensorRetrieve , "data.SensorRetrievalAttempt", conn, keep.guid = FALSE)


## flow discharge activity table - works!
db$DischargeFlow <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         FlowConditionID = FlowCondition,
         Notes = DischargeNotes) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

dischargeFlow.keys <-uploadData(db$DischargeFlow , "data.DischargeActivity", conn, keep.guid = FALSE)


## estimated discharge table - Works!
db$DischargeEst <- visit %>% 
  inner_join(dischargeFlow.keys, by = c("globalid" = "GlobalID")) %>% 
  select(DischargeActivityID = ID,
         GlobalID = globalid,
         DischargeMethod,
         DischargeEstimatedClassID = EstimatedDischarge_L_per_sec) %>% 
  filter(DischargeMethod =="EST") %>% 
  select(-DischargeMethod)

dischargeEst.keys <-uploadData(db$DischargeEst , "data.DischargeEstimatedObservation", conn, keep.guid = FALSE)


## Discharge volumetric table - works!
db$DischargeVol <- visit %>% 
  inner_join(dischargeFlow.keys, by = c("globalid" = "GlobalID")) %>% 
  filter(DischargeMethod !="EST") %>% 
  left_join(fillTime, by = c("globalid" = "parentglobalid")) %>% 
  select(DischargeActivityID = ID,
         GlobalID = globalid,
         ContainerVolume_mL,
         FillTime_seconds = FillTime_sec,
         EstimatedCapture_percent = EstimatedCapture_Percent)

dischargeVOL.keys <-uploadData(db$DischargeVol , "data.DischargeVolumetricObservation", conn, keep.guid = FALSE)

## Spring Brook dimensions -works!
db$SpringbrookDim <- visit %>% 
  inner_join(dischargeFlow.keys, by = c("globalid" = "GlobalID")) %>% 
  select(DischargeActivityID = ID,
         GlobalID = globalid,
         SpringbrookLengthFlagID = SPBKLength,
         SpringbrookLength_m = Length_m,
         SpringbrookWidth_m = Width_m,
         Notes = ChannelDescription)

SpringbrookDim.keys <-uploadData(db$SpringbrookDim , "data.SpringbrookDimensions", conn, keep.guid = FALSE)


## WaterQualityActivity table - works!
db$WQactivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  filter(WasWaterQualityDataCollected ==1) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         WaterQualityDataCollectedID = WasWaterQualityDataCollected,
         pHInstrumentID = pHInstrument,
         DOInstrumentID = DOInstrument,
         SpCondInstrumentID = SpCondInstrument,
         TemperatureInstrumentID = TemperatureInstrument,
         Notes = WQNotes) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

WQactivity.keys <-uploadData(db$WQactivity, "data.WaterQualityActivity", conn, keep.guid = FALSE)


## Water Quality DO table - works!
DO1 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         DissolvedOxygen_mg_per_L = DissolvedOxygen_mg_per_L_1,
         DissolvedOxygen_percent = DissolvedOxygen_percent_1,
         DataQualityFlagID = DO_1Flag) %>% 
  filter(!is.na (DataQualityFlagID))

DO2 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         DissolvedOxygen_mg_per_L = DissolvedOxygen_mg_per_L_2,
         DissolvedOxygen_percent = DissolvedOxygen_percent_2,
         DataQualityFlagID = DO_2Flag)%>% 
  filter(!is.na (DataQualityFlagID))

DO3 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         DissolvedOxygen_mg_per_L = DissolvedOxygen_mg_per_L_3,
         DissolvedOxygen_percent = DissolvedOxygen_percent_3,
         DataQualityFlagID = DO_3Flag)%>% 
  filter(!is.na (DataQualityFlagID))

db$WQ_DO <- rbind(DO1, DO2, DO3) %>% 
  arrange(WaterQualityActivityID)

WQ_DO.keys <-uploadData(db$WQ_DO, "data.WaterQualityDO", conn, keep.guid = FALSE)


## Water Quality pH table - works!
pH1 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         pH = pH_1,
         DataQualityFlagID = pH_1Flag) %>% 
  filter(!is.na (DataQualityFlagID))

pH2 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         pH = pH_2,
         DataQualityFlagID = pH_2Flag) %>% 
  filter(!is.na (DataQualityFlagID))

pH3 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         pH = pH_3,
         DataQualityFlagID = pH_3Flag) %>% 
  filter(!is.na (DataQualityFlagID))

db$WQ_pH <- rbind(pH1, pH2, pH3) %>% 
  arrange(WaterQualityActivityID)

WQ_pH.keys <-uploadData(db$WQ_pH, "data.WaterQualitypH", conn, keep.guid = FALSE)


## Water Quality SpCond table - works!
spCond1 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         SpecificConductance_microS_per_cm = SpecificConductance_microS_1,
         DataQualityFlagID = SpCond_microS_1Flag) %>% 
  filter(!is.na (DataQualityFlagID))

spCond2 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         SpecificConductance_microS_per_cm = SpecificConductance_microS_2,
         DataQualityFlagID = SpCond_microS_2Flag) %>% 
  filter(!is.na (DataQualityFlagID))

spCond3 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         SpecificConductance_microS_per_cm = SpecificConductance_microS_3,
         DataQualityFlagID = SpCond_microS_3Flag) %>% 
  filter(!is.na (DataQualityFlagID))

db$WQ_SpCond<- rbind(spCond1, spCond2, spCond3) %>% 
  arrange(WaterQualityActivityID)

WQ_SpCond.keys <-uploadData(db$WQ_SpCond, "data.WaterQualitySpCond", conn, keep.guid = FALSE)



## Water Quality Temp C table - works!
tempC1 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         WaterTemperature_C = Temperature_C_1,
         DataQualityFlagID = Temp_C_1Flag) %>% 
  filter(!is.na (DataQualityFlagID))

tempC2 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         WaterTemperature_C = Temperature_C_2,
         DataQualityFlagID = Temp_C_2Flag) %>% 
  filter(!is.na (DataQualityFlagID))

tempC3 <- visit %>% 
  inner_join(WQactivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(WaterQualityActivityID = ID,
         WaterTemperature_C = Temperature_C_3,
         DataQualityFlagID = Temp_C_3Flag) %>% 
  filter(!is.na (DataQualityFlagID))


db$WQ_tempC<- rbind(tempC1, tempC2, tempC3) %>% 
  arrange(WaterQualityActivityID)

WQ_tempC.keys <-uploadData(db$WQ_tempC, "data.WaterQualityTemperature", conn, keep.guid = FALSE)



## Disturbance Activity table - works!
db$DisturbanceActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         FlowModificationStatusID =FlowModification,
         Roads,
         HumanUse,
         PlantManagement,
         HikingTrails,
         Livestock,
         OtherAnthropogenic = Other_Anthro,
         Fire,
         Flooding,
         Wildlife,
         OtherNatural = Other_Natural,
         Overall,
         Notes = DisturbanceNotes) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

DisturbanceActivity.keys <-uploadData(db$DisturbanceActivity, "data.DisturbanceActivity", conn, keep.guid = FALSE)


## Disturbance Flow Modification table - works!
db$DisturbanceMod <-disturbanceFlowMod%>% 
  inner_join(DisturbanceActivity.keys, by = c("parentglobalid" = "GlobalID")) %>% 
  select(DisturbanceActivityID = ID,
         GlobalID = globalid,
         ModificationTypeID = ModificationType)

DisturbanceMod.keys <-uploadData(db$DisturbanceMod, "data.DisturbanceFlowModification", conn, keep.guid = FALSE)


# Wildlife Activity table - works!
db$WildlifeActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  filter(Waswildlifeobserved ==1) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         IsWildlifeObservedID = Waswildlifeobserved) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

WildlifeActivity.keys <-uploadData(db$WildlifeActivity, "data.WildlifeActivity", conn, keep.guid = FALSE)


## wildlife observation table _ works
db$WildlifeObservation <- wildlife %>% 
  inner_join(WildlifeActivity.keys, by = c("parentglobalid" = "GlobalID")) %>% 
  select(WildlifeActivityID= ID,
         GlobalID = globalid,
         WildlifeTypeID = WildlifeType,
         DirectObservation,
         Scat,
         Tracks,
         Shelter,
         Foraging,
         Vocalization,
         OtherEvidence,
         Notes = Species_Notes) %>% 
  mutate_at(c(3:10), ~replace(., is.na(.), 3))


WildlifeObservation.keys <-uploadData(db$WildlifeObservation , "data.WildlifeObservation", conn, keep.guid = FALSE)


# Riparian veg Activity table - works! filters on if riparian veg was observed
db$riparianVegActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  filter(WasRiparianVegetationObserved ==1) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         IsVegetationObservedID = WasRiparianVegetationObserved,
         MistletoePresentID =  MistletoePresent,
         Notes = RiparianVegetationNotes)
riparianVegActivity.keys <-uploadData(db$riparianVegActivity, "data.RiparianVegetationActivity", conn, keep.guid = FALSE)


## riparian veg observation table - works!
tempveg1<- visit %>% 
  inner_join(riparianVegActivity.keys, by = c("globalid" = "GlobalID")) %>% 
  select(RiparianVegetationActivityID = ID,
         GlobalID = globalid,
         LifeFormList,
         LifeFormListWPhotos,
         WoodyGT4m,
         Woody2to4m,
         WoodyLT2m,
         Forb,
         Grass,
         Reed,
         Rush,
         Sedge,
         Cattail,
         Bryophyte,
         NonPlant ) %>% 
  rename("Woody 2-4m" = Woody2to4m, "Woody <2m" = WoodyLT2m, "Woody >4m" = WoodyGT4m, "Non-Plant" = NonPlant)

tempVeg2 <- gather(tempveg1, "LifeFormName", "Rank", 5:15) %>% 
  filter(Rank !=12) %>% 
  left_join(riparianVeg, by = c("GlobalID" = "parentglobalid", "LifeFormName" = "LifeFormName" )) %>% 
  select(RiparianVegetationActivityID,
         GlobalID,
         LifeFormName,
         Rank,
         DominantSpecies) %>% 
  inner_join(lifeform.types, by = c("LifeFormName" = "Label")) # gets the life form ID from lookup.lifeform

db$riparianVegObservation <- tempVeg2  %>% 
  select(-LifeFormName,
         - Code,
         LifeFormID = ID) %>% 
  mutate( ProtectedStatusID = 4, TaxonomicReferenceAuthorityID = 1) 

riparianVegObservation.keys <-uploadData(db$riparianVegObservation , "data.RiparianVegetationObservation", conn, keep.guid = FALSE)
## there is a non matching record between the visit table and veg repeat table that is creating a null in the table for import - how should we handle these kinds of errors?


## Invasives Activity table - works!
db$InvasiveActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  filter(WereInvasivesObserved == 1) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         InvasivesObservedID = WereInvasivesObserved,
         Notes = InvasiveNotes) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

InvasiveActivity.keys <-uploadData(db$InvasiveActivity, "data.InvasivesActivity", conn, keep.guid = FALSE)

## Invasives Observations table - works!
db$InvasiveObservation <- invasives %>% 
  inner_join(InvasiveActivity.keys, by = c("parentglobalid" = "GlobalID"))%>% 
  inner_join(visit, by = c("parentglobalid" = "globalid")) %>% 
  select(InvasivesActivityID = ID,
         GlobalID = globalid,
         UTMZoneID = UTMZone,
         HorizontalDatumID  = Datum,
         GPSUnitID = GPS,
         GpsX = x,
         GpsY = y,
         TaxonID = InvasiveSpecies,
         RiparianVegetationBufferID = RiparianVegBuffer) %>% 
  #mutate_at(c(3:4), ~replace(., is.na(.), 1)) %>% # delete- only for testingt
  mutate( ProtectedStatusID = 4, TaxonomicReferenceAuthorityID = 1,
          GpsX = round(GpsX,8),
          GpsY = round(GpsY,8)) %>% 
  mutate_at(c(9), ~replace(., is.na(.), 1)) # for testing- delete!!!
## missing a species notes field?

InvasiveObservation.keys <-uploadData(db$InvasiveObservation, "data.InvasivesObservation", conn, keep.guid = FALSE)
# there is a null value in the real invaives table caused by an empty species and taxon ID field in AGOl. fix for the one time import

## Repeat Activity table - works
db$RepeatActivity <-visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         CameraID = Camera,
         CameraCardID = CameraCard) %>% 
  mutate(DataProcessingLevelID = 1)

RepeatActivity.keys <-uploadData(db$RepeatActivity, "data.RepeatPhotoActivity", conn, keep.guid = FALSE)


## Repeat Observations table - works!
db$RepeatObservation <- repeats %>% 
  inner_join(RepeatActivity.keys, by = c("parentglobalid" = "GlobalID"))%>% 
  inner_join(visit, by = c("parentglobalid" = "globalid")) %>% 
  select(RepeatPhotoActivityID = ID,
         GlobalID = globalid,
         RepeatPhotoTypeID = PhotoType,
         Notes = PhotoNotes_ExternalCamera,
         UTMZone,
         HorizontalDatumID  = Datum,
         GPSUnitID = GPS,
         GpsX = x,
         GpsY = y )%>% 
  #mutate_at(c(3:4), ~replace(., is.na(.), 1)) %>% # delete- only for testingt
  mutate(GpsX = round(GpsX,8),
          GpsY = round(GpsY,8))


RepeatObservation.keys <-uploadData(db$RepeatObservation, "data.RepeatPhotoObservation", conn, keep.guid = FALSE)

######################################################################
## Photo Activity table - works! This is for all photos ext and int
db$PhotoActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select( VisitID = ID,
          GlobalID = globalid,
          CameraID = Camera,
          CameraCardID = CameraCard) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

PhotoActivity.keys <-uploadData(db$PhotoActivity, "data.PhotoActivity", conn, keep.guid = FALSE)
names(PhotoActivity.keys) <- c("PhotoActivityID", "VisitGlobalID")

## Photo table upload in 2 stages, internal images and external images
## needs to be done last - get all internal tables first only if there are invasive images
# internal photos - works!

if (any(visit$UsingInternalCamera == "Y")){
  print("Internal")
  # invasive internal photos -
  photo1 <- Inv_IntImage %>% 
    mutate(VisitGUID = tolower(VisitGUID),
           GlobalID = tolower(GlobalID),
           RepeatGUID = tolower(RepeatGUID)) %>%
    inner_join(PhotoActivity.keys, by = c("VisitGUID" = "VisitGlobalID")) %>% 
    inner_join(visit, by = c("VisitGUID" = "globalid")) %>% 
    inner_join(invasives, by = c("RepeatGUID" = "globalid")) %>% 
    inner_join(photo.types, by = c("PhotoType" = "Code")) %>% 
    select(PhotoActivityID,
           StartDateTime,
           OriginalFilePath,
           RenamedFilePath,
           PhotoDescriptionCodeID = ID,
           GPSUnitID = GPS,
           HorizontalDatumID = Datum,
           GpsX = x,
           GpsY = y,
           wkid) %>% 
    mutate(DateTaken = format.Date(StartDateTime, "%Y-%m-%d"),
           IsLibraryPhotoID = 9,
           GpsX = round(GpsX,8),
           GpsY = round(GpsY,8)) 
  
  # repeat internal photos
  photo2 <- Rep_IntImage %>% 
    mutate(VisitGUID = tolower(VisitGUID),
           GlobalID = tolower(GlobalID),
           RepeatGUID = tolower(RepeatGUID)) %>%
    inner_join(PhotoActivity.keys, by = c("VisitGUID" = "VisitGlobalID")) %>% 
    inner_join(visit, by = c("VisitGUID" = "globalid")) %>% 
    inner_join(repeats, by = c("RepeatGUID" = "globalid")) %>% 
    inner_join(photo.types, by = c("PhotoType.x" = "Code")) %>%
    select(PhotoActivityID,
           StartDateTime,
           OriginalFilePath,
           RenamedFilePath,
           PhotoDescriptionCodeID = ID,
           GPSUnitID = GPS,
           HorizontalDatumID = Datum,
           GpsX = x,
           GpsY = y,
           wkid) %>% 
    mutate(DateTaken = format.Date(StartDateTime, "%Y-%m-%d"),
           IsLibraryPhotoID = 9,
           GpsX = round(GpsX,8),
           GpsY = round(GpsY,8))
  
  
  # riparian veg internal photos
  photo3 <- RV_IntImage %>% 
    mutate(VisitGUID = tolower(VisitGUID),
           GlobalID = tolower(GlobalID),
           RepeatGUID = tolower(RepeatGUID)) %>%
    inner_join(PhotoActivity.keys, by = c("VisitGUID" = "VisitGlobalID")) %>% 
    inner_join(visit, by = c("VisitGUID" = "globalid")) %>% 
    inner_join(riparianVeg, by = c("RepeatGUID" = "globalid")) %>% 
    inner_join(photo.types, by = c("PhotoType" = "Code")) %>%
    select(PhotoActivityID,
           StartDateTime,
           OriginalFilePath,
           RenamedFilePath,
           PhotoDescriptionCodeID = ID,
           GPSUnitID = GPS,
           HorizontalDatumID = Datum) %>% 
    mutate(DateTaken = format.Date(StartDateTime, "%Y-%m-%d"),
           IsLibraryPhotoID = 9,
           GpsX = as.numeric(""),
           GpsY = as.numeric(""),
           wkid = as.numeric(""))
  
  # additional internal photos
  photo4 <- AP_IntImage %>% 
    mutate(VisitGUID = tolower(VisitGUID),
           GlobalID = tolower(GlobalID),
           RepeatGUID = tolower(RepeatGUID)) %>%
    inner_join(PhotoActivity.keys, by = c("VisitGUID" = "VisitGlobalID")) %>% 
    inner_join(visit, by = c("VisitGUID" = "globalid")) %>% 
    inner_join(additionalPhotos, by = c("RepeatGUID" = "globalid")) %>% 
    inner_join(photo.types, by = c("PhotoType" = "Code")) %>%
    select(PhotoActivityID,
           StartDateTime,
           OriginalFilePath,
           RenamedFilePath,
           PhotoDescriptionCodeID = ID,
           GPSUnitID = GPS,
           HorizontalDatumID = Datum) %>% 
    mutate(DateTaken = format.Date(StartDateTime, "%Y-%m-%d"),
           IsLibraryPhotoID = 9,
           GpsX = as.numeric(""),
           GpsY = as.numeric(""),
           wkid = as.numeric(""))
  # join tables together 
  #add utm fields ..... to do
  db$PhotoInt <- rbind(photo1, photo2, photo3, photo4) %>% 
    select(-wkid, - StartDateTime)


  PhotoInt.keys <-uploadData(db$PhotoInt, "data.Photo", conn, keep.guid = FALSE)
  
}else{
  print("No internal images to import")
}

# Notes = PhotoNotes?
############################### need to finish! 2 tables and file names########################
#external photos

#invasive - have to double join to look up tables to get photoTypeID
photo5 <- invasivesExt %>% 
  inner_join(invasives, by = c("parentglobalid" = "globalid")) %>% 
  inner_join(visit, by = c("parentglobalid.y" = "globalid")) %>% 
  inner_join(PhotoActivity.keys, by = c("parentglobalid.y" = "VisitGlobalID")) %>% 
  inner_join(taxon.types, by = c("InvasiveSpecies"= "ID")) %>% 
  inner_join(photo.types, by = c("USDAPlantsCode" = "Code")) %>% 
  select(PhotoActivityID,
         StartDateTime,
         GPSUnitID = GPS,
         HorizontalDatumID = Datum,
         GpsX = x,
         GpsY = y,
         wkid,
         ExternalFileNumbersInv, 
         PhotoDescriptionCodeID = ID) %>% 
  mutate(DateTaken = format.Date(StartDateTime, "%Y-%m-%d"),
         IsLibraryPhotoID = 9,
         GpsX = round(GpsX,8),
         GpsY = round(GpsY,8))

# repeat external photos
photo6 <- repeatsExt %>% 
  inner_join(repeats, by = c("parentglobalid" = "globalid")) %>% 
  inner_join(visit, by = c("parentglobalid.y" = "globalid")) %>% 
  inner_join(PhotoActivity.keys, by = c("parentglobalid.y" = "VisitGlobalID")) %>% 
  select(PhotoActivityID,
         StartDateTime,
         GPSUnitID = GPS,
         HorizontalDatumID = Datum,
         GpsX = x,
         GpsY = y,
         wkid,
         ExternalFileNumber, 
         PhotoDescriptionCodeID = PhotoType) %>% 
  mutate(DateTaken = format.Date(StartDateTime, "%Y-%m-%d"),
         IsLibraryPhotoID = 9,
         GpsX = round(GpsX,8),
         GpsY = round(GpsY,8))
  
# riparian veg external photos

# additional external photos

pool::poolClose(conn)
