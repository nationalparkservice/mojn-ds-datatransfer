#################################################################
## Purpose: Read tabular data from the desert springs ArcGIS Online (AGOL)
##          and create dataframes that better match the SQL database. 
##          Must use before data-upload-ds.R
## Date: 09/09/2020
## R version written with: 4.0.2
## Libraries and versions used: libraries specified in Main.R. Additional libraries for this R script: rstudioapi 0.11 
################################################################

date_qry <- "DateTime < DATE '2022-06-07'"

## Get a token with a headless account - Warning! You need to know the password!
token_resp <- POST("https://nps.maps.arcgis.com/sharing/rest/generateToken",
                   body = list(username = rstudioapi::showPrompt("Username", "Please enter your AGOL username", default = "mojn_hydro"),
                               password = rstudioapi::askForPassword("Please enter your AGOL password"),
                               referer = 'https://irma.nps.gov',
                               f = 'json'),
                   encode = "form")
agol_token <- fromJSON(content(token_resp, type="text", encoding = "UTF-8"))

service_url = "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_815059c20fd448628dc23441f7a7c473/FeatureServer"
#service_url = "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_781d06dcb4bc4537b179e6aec279a2ca/FeatureServer"

## Get AGOL desert springs (DS) spring visit point layer: MOJN_DS_SpringVisit
## create visit dataframe
resp.visit <- GET(paste0(service_url, "/0/query"),
                  query = list(where=date_qry,
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))

visit <- fromJSON(content(resp.visit, type = "text", encoding = "UTF-8"))
visit <- visit$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  mutate(DateTime = as.POSIXct(DateTime/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(StartDateTime = DateTime, Survey123_LastEditedDate = EditDate) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))

# visit_parentglobalid_qry <- paste0("parentglobalid IN (", paste0("'", visit$globalid[1:40], "'", collapse = ", "), ")")

#visit_parentglobalid_qry <- "parentglobalid IN ('ed77c029-6c83-42ec-aeab-9ad123d370d5', 'fd444a72-b993-48fe-9118-7fb4f31b84a7')"
## get AGOL DS repeats point layer: Repeats
## create repeatPhoto dataframe
resp.repeats <- GET(paste0(service_url, "/1/query"),
                    query = list(where="1=1",
                                 outFields="*",
                                 f="JSON",
                                 token=agol_token$token))

repeats <- fromJSON(content(resp.repeats, type = "text", encoding = "UTF-8"))
repeats <- cbind(repeats$features$attributes, repeats$features$geometry) %>%
  mutate(wkid = repeats$spatialReference$wkid) %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))

## get AGOL DS invasive plants point layer: InvasivePlants
## create invasives dataframe
resp.invasives <- GET(paste0(service_url, "/2/query"),
                    query = list(where="1=1",
                                 outFields="*",
                                 f="JSON",
                                 token=agol_token$token))

invasives <- fromJSON(content(resp.invasives, type = "text", encoding = "UTF-8"))
invasives <- cbind(invasives$features$attributes, invasives$features$geometry) %>%
  mutate(wkid = invasives$spatialReference$wkid) %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))

## get AGOL DS observers table: Observers
## create observers dataframe
resp.observers <- GET(paste0(service_url, "/3/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))

observers <- fromJSON(content(resp.observers, type = "text", encoding = "UTF-8"))
observers <- observers$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))


## get AGOL DS sensor retrieval table: SensorRetrieval
## create sensorRetrieval dataframe
resp.sensorRetrieval <- GET(paste0(service_url, "/4/query"),
                      query = list(where="1=1",
                                   outFields="*",
                                   f="JSON",
                                   token=agol_token$token))

sensorRetrieval <- fromJSON(content(resp.sensorRetrieval, type = "text", encoding = "UTF-8"))
sensorRetrieval <- sensorRetrieval$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>%
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))


## get AGOL DS repeat photos-internal table: RepeatPhotos_Internal
## create repeatsInt dataframe
resp.repeatsInt <- GET(paste0(service_url, "/5/query"),
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))

repeatsInt <- fromJSON(content(resp.repeatsInt, type = "text", encoding = "UTF-8"))
repeatsInt <- repeatsInt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)
if (nrow(repeatsInt) > 0) {
  repeatsInt %<>%
    filter(parentglobalid %in% repeats$globalid)
}


## get AGOL DS repeat photos-external table: RepeatPhotos_External
## create repeatsExt dataframe
resp.repeatsExt <- GET(paste0(service_url, "/6/query"),
                          query = list(where="1=1",
                                       outFields="*",
                                       f="JSON",
                                       token=agol_token$token))

repeatsExt <- fromJSON(content(resp.repeatsExt, type = "text", encoding = "UTF-8"))
repeatsExt <- repeatsExt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  filter(parentglobalid %in% repeats$globalid)


## get AGOL DS fill time table: FillTime
## create fillTime dataframe
resp.fillTime <- GET(paste0(service_url, "/7/query"),
                          query = list(where="1=1",
                                       outFields="*",
                                       f="JSON",
                                       token=agol_token$token))

fillTime <- fromJSON(content(resp.fillTime, type = "text", encoding = "UTF-8"))
fillTime <- fillTime$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))


## get AGOL DS Flow Mod Types table: FlowModTypes
## create disturbanceFlowMod dataframe
resp.disturbanceFlowMod <- GET(paste0(service_url, "/8/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))

disturbanceFlowMod <- fromJSON(content(resp.disturbanceFlowMod, type = "text", encoding = "UTF-8"))
disturbanceFlowMod <- disturbanceFlowMod$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))


## get AGOL DS Wildlife repeat table: WildlifeRepeat
## create wildlife dataframe
resp.wildlife <- GET(paste0(service_url, "/9/query"),
                    query = list(where="1=1",
                                 outFields="*",
                                 f="JSON",
                                 token=agol_token$token))

wildlife <- fromJSON(content(resp.wildlife, type = "text", encoding = "UTF-8"))
wildlife <- wildlife$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))


## get AGOL DS riparian Vegetation image repeat table: VegImageRepeat
## create riparianVeg dataframe
resp.riparianVeg  <- GET(paste0(service_url, "/10/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))

riparianVeg  <- fromJSON(content(resp.riparianVeg, type = "text", encoding = "UTF-8"))
riparianVeg  <- riparianVeg$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(EditDate = as.POSIXct(EditDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(Survey123_LastEditedDate = EditDate) %>% 
  filter(parentglobalid %in% visit$globalid) %>% 
  mutate(Survey123_LastEditedDate = as.character(Survey123_LastEditedDate))

## get AGOL DS riparian veg internal camera table: InternalCamera
## create riparianVegInt dataframe
resp.riparianVegInt <- GET(paste0(service_url, "/11/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))

riparianVegInt <- fromJSON(content(resp.riparianVegInt, type = "text", encoding = "UTF-8"))
riparianVegInt <- riparianVegInt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)
if (nrow(riparianVegInt) > 0) {
  riparianVegInt %<>%
    filter(parentglobalid %in% riparianVeg$globalid)
}


## get AGOL DS riparian veg external Camera table: ExternalCameraFiles
## create riparianVegExt dataframe
resp.riparianVegExt <- GET(paste0(service_url, "/12/query"),
                           query = list(where="1=1",
                                        outFields="*",
                                        f="JSON",
                                        token=agol_token$token))

riparianVegExt <- fromJSON(content(resp.riparianVegExt, type = "text", encoding = "UTF-8"))
riparianVegExt <- riparianVegExt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  filter(parentglobalid %in% riparianVeg$globalid)


## get AGOL DS Invasive Image repeat table: InvImageRepeat
## create invasivesInt dataframe
resp.invasivesInt <- GET(paste0(service_url, "/13/query"),
                           query = list(where="1=1",
                                        outFields="*",
                                        f="JSON",
                                        token=agol_token$token))

invasivesInt <- fromJSON(content(resp.invasivesInt, type = "text", encoding = "UTF-8"))
invasivesInt <- invasivesInt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)
if (nrow(invasivesInt) > 0) {
  invasivesInt %<>%
    filter(parentglobalid %in% invasives$globalid)
}

## get AGOL DS Inv External CAmera Files table: ExternalCameraFilesInv
## create invasivesExt dataframe
resp.invasivesExt  <- GET(paste0(service_url, "/14/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))

invasivesExt  <- fromJSON(content(resp.invasivesExt , type = "text", encoding = "UTF-8"))
invasivesExt  <- invasivesExt $features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  filter(parentglobalid %in% invasives$globalid)


## get AGOL DS Additional Photos Files table: AdditionalPhotos2
## create additionalPhotos dataframe
resp.additionalPhotos<- GET(paste0(service_url, "/15/query"),
                              query = list(where="1=1",
                                           outFields="*",
                                           f="JSON",
                                           token=agol_token$token))

additionalPhotos <- fromJSON(content(resp.additionalPhotos, type = "text", encoding = "UTF-8"))
additionalPhotos <- additionalPhotos$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  filter(parentglobalid %in% visit$globalid)

## get AGOL DS Additional Photos Internal Files table: AdditionalPhotoInternal
## create additionalPhotosInt dataframe
resp.additionalPhotosInt<- GET(paste0(service_url, "/16/query"),
                            query = list(where="1=1",
                                         outFields="*",
                                         f="JSON",
                                         token=agol_token$token))

additionalPhotosInt <- fromJSON(content(resp.additionalPhotosInt, type = "text", encoding = "UTF-8"))
additionalPhotosInt <- additionalPhotosInt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)
if (nrow(additionalPhotosInt) > 0) {
  additionalPhotosInt %<>%
    filter(parentglobalid %in% additionalPhotos$globalid)
}

## get AGOL DS Additional Photos External Files table: AddtionalPhotoExternal
## create additionalPhotosExt dataframe
resp.additionalPhotosExt<- GET(paste0(service_url, "/17/query"),
                               query = list(where="1=1",
                                            outFields="*",
                                            f="JSON",
                                            token=agol_token$token))

additionalPhotosExt <- fromJSON(content(resp.additionalPhotosExt, type = "text", encoding = "UTF-8"))
additionalPhotosExt <- additionalPhotosExt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  filter(parentglobalid %in% additionalPhotos$globalid)


## this line if uncommented can remove all the tem,p "resp." list files and
## make it easier to see the tables in the global environment. can be useful for testing or modyfying the code
# rm(list=ls(pattern="^resp."))

