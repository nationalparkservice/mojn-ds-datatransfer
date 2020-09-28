## Notes on using ArcGIS Pro for python/arcpy needed in the python script "download-photos-ds.py":
## Because ArcGIS Pro stores ArcPy away from its python folder, you need to make sure the following 
## directories are added to your environment variables.
##  1. Go to the Start menu and type "edit the environment variables for your account"
##  2. Click on the path then add, and add the following:
##        C:\Program Files\ArcGIS\Pro\Resources\ArcPy
##        C:\Program Files\ArcGIS\Pro\Resources\ArcToolbox\Scripts
##        C:\Program Files\ArcGIS\Pro\bin
##  3. If you need to connect to the network to use ArcPro (you don't have a licence checked out
##    for offline use) you will need to launch ArcPro to be able to use ArcPy
##  4. If you have more than one Python instance on your machine, for example ArcGIS and full python,
##     you need to force the reticyuate package to use the ArcGIS version BEFORE you load the library:
Sys.setenv(RETICULATE_PYTHON = "C:\\Program Files\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3")
library(reticulate)

library(jsonlite)
library(httr)
library(tidyverse)
library(pool)
library(dbplyr)
library(DBI)
library(stringr)
source("utils-ds.R")

source("agol-download-ds.R")
source("data-upload-ds.R")

#check the python path
repl_python()
#check ArcPy works!
import("arcpy")
