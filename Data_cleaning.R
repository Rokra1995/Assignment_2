#Database management and Digital Tools
#Environment####
if(!require('rstudioapi') ){ install.packages('rstudioapi') }
library(rstudioapi)

if(!require('tidyverse') ){ install.packages('tidyverse') }
library(tidyverse)

if(!require('GGally') ){ install.packages('GGally') }
library("GGally")

if(!require('corrplot')) install.packages('corrplot')
library(corrplot)

if(!require('haven')) install.packages('haven')
library(haven)

if(!require('dplyr')) install.packages('dplyr')
library(dplyr)

if(!require('varhandle')) install.packages('varhandle')
library(varhandle)

if(!require('broom')) install.packages('broom')
library(broom)

if(!require('car')) install.packages('car')
library(car)
library("readxl")

#Directory####
setwd(dirname(getSourceEditorContext()$path))

#Data Cleaning####
data <- read_csv("/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/housing_data.csv")

data <- read_csv("/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/cbs_data.csv", sep=';')
#overview
summary(data)

# change misclassified strings to numeric
data <- transform(data, koopPrijs = as.numeric(koopPrijs), 
          aantalBadkamers = as.numeric(aantalBadkamers),
          perceelOppervlakte = as.numeric(perceelOppervlakte),
          energielabelKlasse = as.factor(energielabelKlasse))
summary(data)
# select necessary columns
data <- data %>% select(globalId, publicatieDatum, postcode, koopPrijs, volledigeOmschrijving, soortWoning, categorieObject, bouwjaar, indTuin, perceelOppervlakte, aantalKamers, aantalBadkamers, energielabelKlasse, oppervlakte, datum_ondertekening)
summary(data)





