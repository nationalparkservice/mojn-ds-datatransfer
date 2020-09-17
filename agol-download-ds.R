#################################################################
## Purpose: Read tabular data from the desert springs AGOL and
##          prepare for import into the desert springs SQL database
## Date: 09/09/2020
## R version written with: 4.0.2
## Libraries and versions used: rstudioapi 0.11
################################################################

## Get a token with a headless account
token_resp <- POST("https://nps.maps.arcgis.com/sharing/rest/generateToken",
                   body = list(username = rstudioapi::showPrompt("Username", "Please enter your AGOL username", default = "mojn_hydro"),
                               password = rstudioapi::askForPassword("Please enter your AGOL password"),
                               referer = 'https://irma.nps.gov',
                               f = 'json'),
                   encode = "form")
agol_token <- fromJSON(content(token_resp, type="text", encoding = "UTF-8"))

service_url = "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_815059c20fd448628dc23441f7a7c473/FeatureServer"


## Get desert springs spring visit data
resp.visit <- GET(paste0(service_url, "/0/query"),
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))

visit <- fromJSON(content(resp.visit, type = "text", encoding = "UTF-8"))
visit <- visit$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(DateTime = as.POSIXct(DateTime/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(StartDateTime = DateTime)

## get DS repeats data
resp.repeats <- GET(paste0(service_url, "/1/query"),
                    query = list(where="1=1",
                                 outFields="*",
                                 f="JSON",
                                 token=agol_token$token))

repeats <- fromJSON(content(resp.repeats, type = "text", encoding = "UTF-8"))
repeats <- repeats$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS invasive plants data
resp.invasive <- GET(paste0(service_url, "/2/query"),
                    query = list(where="1=1",
                                 outFields="*",
                                 f="JSON",
                                 token=agol_token$token))

invasive <- fromJSON(content(resp.invasive, type = "text", encoding = "UTF-8"))
invasive <- invasive$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS observers table data
resp.observers <- GET(paste0(service_url, "/3/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))

observers <- fromJSON(content(resp.observers, type = "text", encoding = "UTF-8"))
observers <- observers$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS sensor retrieval table data
resp.sensor <- GET(paste0(service_url, "/4/query"),
                      query = list(where="1=1",
                                   outFields="*",
                                   f="JSON",
                                   token=agol_token$token))

sensor <- fromJSON(content(resp.sensor, type = "text", encoding = "UTF-8"))
sensor <- sensor$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS repeat photos-internal table data
resp.repeatPhotosInt <- GET(paste0(service_url, "/5/query"),
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))

repeatPhotosInt <- fromJSON(content(resp.repeatPhotosInt, type = "text", encoding = "UTF-8"))
repeatPhotosInt <- repeatPhotosInt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)


## get DS repeat photos-external table data
resp.repeatPhotosExt <- GET(paste0(service_url, "/6/query"),
                          query = list(where="1=1",
                                       outFields="*",
                                       f="JSON",
                                       token=agol_token$token))


repeatPhotosExt <- fromJSON(content(resp.repeatPhotosExt, type = "text", encoding = "UTF-8"))
repeatPhotosExt <- repeatPhotosExt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)


## get DS fill time table data
resp.fillTime <- GET(paste0(service_url, "/7/query"),
                          query = list(where="1=1",
                                       outFields="*",
                                       f="JSON",
                                       token=agol_token$token))


fillTime <- fromJSON(content(resp.fillTime, type = "text", encoding = "UTF-8"))
fillTime <- fillTime$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Flow Mod Types table data
resp.flowMod <- GET(paste0(service_url, "/8/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))


flowMod <- fromJSON(content(resp.flowMod, type = "text", encoding = "UTF-8"))
flowMod <- flowMod$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Wildlife repeat table data
resp.wildlife <- GET(paste0(service_url, "/9/query"),
                    query = list(where="1=1",
                                 outFields="*",
                                 f="JSON",
                                 token=agol_token$token))


wildlife <- fromJSON(content(resp.wildlife, type = "text", encoding = "UTF-8"))
wildlife <- wildlife$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Veg image repeat table data
resp.vegImage <- GET(paste0(service_url, "/10/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))


vegImage <- fromJSON(content(resp.vegImage, type = "text", encoding = "UTF-8"))
vegImage <- vegImage$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Internal Camera Table data
resp.cameraInternal <- GET(paste0(service_url, "/11/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))


cameraInternal <- fromJSON(content(resp.cameraInternal, type = "text", encoding = "UTF-8"))
cameraInternal <- cameraInternal$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)


## get DS External Camera table data
resp.cameraExternal <- GET(paste0(service_url, "/12/query"),
                           query = list(where="1=1",
                                        outFields="*",
                                        f="JSON",
                                        token=agol_token$token))


cameraExternal <- fromJSON(content(resp.cameraExternal, type = "text", encoding = "UTF-8"))
cameraExternal <- cameraExternal$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Inv Image repeat table data
resp.invImage <- GET(paste0(service_url, "/13/query"),
                           query = list(where="1=1",
                                        outFields="*",
                                        f="JSON",
                                        token=agol_token$token))


invImage <- fromJSON(content(resp.invImage, type = "text", encoding = "UTF-8"))
invImage <- invImage$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Inv External CAmera Files table data
resp.extCameraFilesInv <- GET(paste0(service_url, "/14/query"),
                     query = list(where="1=1",
                                  outFields="*",
                                  f="JSON",
                                  token=agol_token$token))


extCameraFilesInv <- fromJSON(content(resp.extCameraFilesInv, type = "text", encoding = "UTF-8"))
extCameraFilesInv <- extCameraFilesInv$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)


## get DS Additional Photos Files table data
resp.additionalPhotos<- GET(paste0(service_url, "/15/query"),
                              query = list(where="1=1",
                                           outFields="*",
                                           f="JSON",
                                           token=agol_token$token))


additionalPhotos <- fromJSON(content(resp.additionalPhotos, type = "text", encoding = "UTF-8"))
additionalPhotos <- additionalPhotos$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## get DS Additional Photos Internal Files table data
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

## get DS Additional Photos External Files table data
resp.additionalPhotosExt<- GET(paste0(service_url, "/17/query"),
                               query = list(where="1=1",
                                            outFields="*",
                                            f="JSON",
                                            token=agol_token$token))


additionalPhotosExt <- fromJSON(content(resp.additionalPhotosExt, type = "text", encoding = "UTF-8"))
additionalPhotosExt <- additionalPhotosExt$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)
