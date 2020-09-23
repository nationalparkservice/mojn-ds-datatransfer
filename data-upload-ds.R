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
gdb.path <- "C:\\Users\\EEdson\\Desktop\\MOJN\\MOJN_DS_Spring_LizzyTest.gdb"
photo.dest <- "C:\\Users\\EEdson\\Desktop\\MOJN\\Photos_DS"
originals.dest <- "C:\\Users\\EEdson\\Desktop\\MOJN\\Photo_Originals"
db.params.path <- "C:\\Users\\EEdson\\Desktop\\MOJN\\mojn-ds-datatransfer\\ds-database-conn.csv"

# filepaths for the real import:
# gdb.path <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\FieldData\\Lakes_Annual\\STLK_AnnualLakeVisit_20191022.gdb"
# photo.dest <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\ImageData\\Lakes"
# originals.dest <- "M:\\MONITORING\\_FieldPhotoOriginals_DoNotModify\\AGOL_STLK"
# db.params.path <- "C:\\Users\\EEdson\\Desktop\\MOJN\\mojn-ds-datatransfer\\ds-database-conn.csv"
#---------------------------#

db <- list()

## Get Site table from database
params <- readr::read_csv(db.params.path) %>% 
  as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)
sites <- dplyr::tbl(conn, dbplyr::in_schema("data", "Site")) %>%
  dplyr::collect()


# Get lookups of photo codes for photo naming. We need both the photo type and the photo SOP type for connecting I think. I will test both.

# original photo type variable
photo.types <- dplyr::tbl(conn, dbplyr::in_schema("ref", "PhotoDescriptionCode")) %>%
  dplyr::collect() %>%
  select(ID, Code)
# new one that conbines photo SOP. incase we need this
photo.types1 <- dplyr::tbl(conn, dbplyr::in_schema("ref", "PhotoDescriptionCode")) %>%
  dplyr::collect() %>%
  select(ID, Code, PhotoSOPID)

photo.types2 <-dplyr::tbl(conn, dbplyr::in_schema("lookup", "photoSOP")) %>%
  dplyr::collect() %>%
  select(ID, Code)
names(photo.types2) <- c("PhotoSOPID", "PhotoSOP_Code")

photoTypes <- photo.types1 %>% 
  left_join(photo.types2, by = "PhotoSOPID")


######################################################################################
## Download photos using Python script
visit.data <- paste(gdb.path, "MOJN_DS_SpringVisit", sep = "\\")
py.ver <- py_config()

# nif (!(py.ver$version < 3)) {
#   use_python("C:\\Python27\\ArcGISx6410.5", required = TRUE)
# }

source_python("download-photos-ds.py")

## Create Python dictionaries from photo and site lookups- don't need these because the site coddes are in the visit ID, and there are no photo types NB trying to add the photo codes back in
#photo.type.dict <- py_dict(photo.types$ID, photo.types$Code)
#site.code.dict <- py_dict(sites$ID, sites$Code)


## Download photos from internal camera table this is the  riparian veg internal camera - Works!
if(nrow(riparianVegInt) == 0){
  print("There are no Riparian Veg internal camera images")
}else{
  photo.type.field <- "LifeFormName"
  photo.table <- paste(gdb.path, "InternalCamera__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "InternalCamera", sep = "\\")
  repeat.data <- paste(gdb.path, "VegImageRepeat", sep = "\\")
  RV_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, repeatFeatureClass = repeat.data,
                                                 visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                                 originalsLocation = originals.dest, photoTypeField = photo.type.field)
  RV_IntImage <- as_tibble(RV_IntImage)
  RV_IntImage$VisitGUID <- str_remove_all(RV_IntImage$VisitGUID, "\\{|\\}")
  RV_IntImage$GlobalID <- str_remove_all(RV_IntImage$GlobalID, "\\{|\\}")
  RV_IntImage$RepeatGUID <- str_remove_all(RV_IntImage$RepeatGUID, "\\{|\\}")
}
    

## Download photos from Inv Image repeat table - works! these are the invasive internal camera images
if(nrow(invasivesInt) == 0){
  print("There are no invasive internal camera images")
}else{
  photo.type.field <- "InvasiveSpeciesCode"
  photo.table <- paste(gdb.path, "InvImageRepeat__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "InvImageRepeat", sep = "\\")
  repeat.data <- paste(gdb.path, "InvasivePlants", sep = "\\")
  Inv_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data,  repeatFeatureClass = repeat.data,
                                                 visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                                 originalsLocation = originals.dest, photoTypeField = photo.type.field)
  Inv_IntImage <- as_tibble(Inv_IntImage)
  Inv_IntImage$VisitGUID <- str_remove_all(Inv_IntImage$VisitGUID, "\\{|\\}")
  Inv_IntImage$GlobalID <- str_remove_all(Inv_IntImage$GlobalID, "\\{|\\}")
}


## Download photos from repeat photos internal table- works!
if(nrow(repeatsInt) == 0){
  print("There are no repeat photos internal images")
}else{
  photo.type.field <- "PhotoTypeName"
  photo.table <- paste(gdb.path, "RepeatPhotos_Internal__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "RepeatPhotos_Internal", sep = "\\")
  repeat.data <- paste(gdb.path, "Repeats", sep = "\\")
  RepeatPhotosInternal <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, repeatFeatureClass = repeat.data,
                                                 visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                                 originalsLocation = originals.dest, photoTypeField = photo.type.field)
  RepeatPhotosInternal <- as_tibble(RepeatPhotosInternal)
  RepeatPhotosInternal$VisitGUID <- str_remove_all(RepeatPhotosInternal$VisitGUID, "\\{|\\}")
  RepeatPhotosInternal$GlobalID <- str_remove_all(RepeatPhotosInternal$GlobalID, "\\{|\\}")
}

## Download photos from Additional Photo Internal table - this needs the phototype dict as a cod is stored not the name.so it uses a differnt python code Unless we can store the name code in there too!
if(nrow(additionalPhotosInt) == 0){
  print("There are no additional internal photo images")
}else{
  photo.table <- paste(gdb.path, "AdditionalPhotoInternal__ATTACH", sep = "\\")
  photo.data <- paste(gdb.path, "AdditionalPhotoInternal", sep = "\\")
  AP_IntImage <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, repeatFeatureClass = repeat.data,
                                       visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, 
                                       originalsLocation = originals.dest, photoTypeField = photo.type.field)
  AP_IntImage <- as_tibble(AP_IntImage)
  AP_IntImage$VisitGUID <- str_remove_all(AP_IntImage$VisitGUID, "\\{|\\}")
  AP_IntImage$GlobalID <- str_remove_all(AP_IntImage$GlobalID, "\\{|\\}")
}

##################################################################################################

## upload table data

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

## TODO repeats. this is repeat photos anot sure how to tie ext and int photos together

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

db$DisturbanceMod <-flowMod %>% 
  inner_join(DisturbanceActivity.keys, by = c("parentglobalid" = "GlobalID")) %>% 
  select(DisturbanceActivityID = ID,
         GlobalID = globalid,
         ModificationTypeID = ModificationType)

DisturbanceMod.keys <-uploadData(db$DisturbanceMod, "data.DisturbanceFlowModification", conn, keep.guid = FALSE)


# Wildlife Activity table - works!
db$WildlifeActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         IsWildlifeObservedID = Waswildlifeobserved) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

WildlifeActivity.keys <-uploadData(db$WildlifeActivity, "data.WildlifeActivity", conn, keep.guid = FALSE)


## wildlife observation table _ check with Sarah about the mutate to replace missing values before running, but it should work fine
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


# Riparian veg Activity table - works!
db$riparianVegActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         IsVegetationObservedID = WasRiparianVegetationObserved,
         MistletoePresentID =  MistletoePresent,
         Notes = RiparianVegetationNotes)
riparianVegActivity.keys <-uploadData(db$riparianVegActivity, "data.RiparianVegetationActivity", conn, keep.guid = FALSE)


## riparian veg observation table - should work but need to confer with Sarah We rename the fields to match the riparian veg fieldds. but 

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
  left_join(vegImage, by = c("GlobalID" = "parentglobalid", "LifeFormName" = "LifeFormName" )) %>% 
  select(RiparianVegetationActivityID,
         GlobalID,
         LifeFormName,
         LifeFormID = LifeForm,
         Rank,
         DominantSpecies )

db$riparianVegObservation <- tempVeg2  %>% 
  select(-LifeFormName) %>% 
  mutate( ProtectedStatusID = 4, TaxonomicReferenceAuthorityID = 1) # ask Sarah where these should come from as there is no taxon ID in this table

riparianVegObservation.keys <-uploadData(db$riparianVegObservation , "data.RiparianVegetationObservation", conn, keep.guid = FALSE)
## there is a non matching record between the visit table and veg repeat table that is creating a null in the table for import - how should we handle these kinds of errors?




## then need to get the riparian photo table. For this we still observation ID, but have to tie back into get the life form photos field and split it out somehow


## Invasives Activity table - works!
db$InvasiveActivity <- visit %>% 
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>% 
  select(VisitID = ID,
         GlobalID = globalid,
         InvasivesObservedID = WereInvasivesObserved,
         Notes = InvasiveNotes) %>% 
  mutate(DataProcessingLevelID = 1)  # Raw

InvasiveActivity.keys <-uploadData(db$InvasiveActivity, "data.InvasivesActivity", conn, keep.guid = FALSE)

## Invasives Observations table
db$InvasiveObservation <- invasive %>% 
  inner_join(InvasiveActivity.keys, by = c("parentglobalid" = "GlobalID"))%>% 
  select(InvasivesActivityID = ID,
         GlobalID = globalid,
         TaxonID = InvasiveSpecies,
         RiparianVegetationBufferID = RiparianVegBuffer) %>% 
  #mutate_at(c(3:4), ~replace(., is.na(.), 1)) %>% # delete- only for testingt
  mutate( ProtectedStatusID = 4, TaxonomicReferenceAuthorityID = 1) # ask Sarah where these should come from as there is no taxon ID in this table
## missing a species notes field? Also utm's, the invasive table is a point layer but I cant see the coords in the table.


InvasiveObservation.keys <-uploadData(db$InvasiveObservation, "data.InvasivesObservation", conn, keep.guid = FALSE)
# there is a null value in the invaives table caused by an empty species and taxon ID field in AGOl. How should we handle these?



## Invasive Photo (external) table
db$InvasivePhotos <- extCameraFilesInv %>% 
  inner_join(InvasiveObservation.keys, by = c("parentglobalid" = "GlobalID")) %>% 
  select(InvasivesObservationID= ID,
         GlobalID = globalid,
         PhotoFileNumber = ExternalFileNumbersInv)

InvasivePhotos.keys <-uploadData(db$InvasivePhotos, "data.InvasivesPhoto", conn, keep.guid = FALSE)



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

## Photo table ( this looks like its for internal photos downloaded to files, rather than external photo file numbers?)

# invasive internal photos
photo1 <- InvImageRepeat %>% 
  mutate(VisitGUID = tolower(VisitGUID),
         GlobalID = tolower(GlobalID)) %>%
  inner_join(PhotoActivity.keys, by = c("VisitGUID" = "VisitGlobalID")) %>% 
  inner_join(visit, by = c("VisitGUID" = "globalid")) %>% 
  select(PhotoActivityID,
         StartDateTime,
         OriginalFilePath,
         RenamedFilePath,
         GPSUnit = GPS) %>% 
  mutate(VisitDate = format.Date(StartDateTime, "%Y-%m-%d"))


# where are these?
# PhotoDescriptionCodeID = PhotoTypeID,
# IsLibraryPhotoID = IsLibrary,  
# X_coord = x,
# Y_coord = y,
# WKID = wkid,
# Notes = PhotoNotes)


#################################################### from STLK script left for now but will remove ######
# ## PhotoActivity table
# db$PhotoActivity <- visit %>%  # All the photo activity data actually just comes from visit!
#   inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
#   select(VisitID = ID,
#          GlobalID = globalid,
#          CameraID,
#          CameraCardID) %>%
#   unique()
# photoact.keys <- uploadData(db$PhotoActivity, "data.PhotoActivity", conn, keep.guid = FALSE)
# names(photoact.keys) <- c("PhotoActivityID", "VisitGlobalID")
# 
# ## Photo table
# db$Photo <- annual_photos %>%
#   mutate(VisitGUID = tolower(VisitGUID),
#          GlobalID = tolower(GlobalID)) %>%
#   inner_join(photoact.keys, by = c("VisitGUID" = "VisitGlobalID")) %>%
#   inner_join(photos, by = c('GlobalID' = 'globalid')) %>%
#   select(PhotoActivityID,
#          PhotoDescriptionCodeID = PhotoTypeID,
#          IsLibraryPhotoID = IsLibrary,
#          OriginalFilePath,
#          RenamedFilePath,
#          GPSUnit = GPS_Photo,
#          X_coord = x,
#          Y_coord = y,
#          WKID = wkid,
#          Notes = PhotoNotes)
# photo.keys <- uploadData(db$Photo, "data.Photo", conn, keep.guid = FALSE)

###############################################################################################################

pool::poolClose(conn)
