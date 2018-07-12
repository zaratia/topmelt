########################################################################################################################
#
#   TOPmelt 1.0
#
########################################################################################################################

########################################################################################################################
#   Import modules
import pyodbc
import os
import sys
import datetime

########################################################################################################################
#   Import user set external functions
from DBreadFunctions import readrun, readbasin, readbandsnum, readbandsprop, readclassesnum, readeiswitches
from DBreadFunctions import readeiswitchdates, readclassesprop, readsnowparams, readenergyindex, readsnowmapsnum
from DBreadFunctions import readraswedates, readsnowinitvar, readmeteoinput, readsunsetsunrise, readeicorr, readparams
from DBwriteFunctions import writerasweresults, writesnowresults, writesnowstatevar
from TOPmeltTools import mapidentification, classvarcorrect
from TOPmeltModels import snowmodel

########################################################################################################################
#   MYSQL TABLES
runsTable = 'RUNS'
basinsTable = 'BASINS'
bandsTable = 'ELEVBNDS'
classesTable = 'ENERGYBNDS'
eiwitchTable = 'EISWITCH'
snowparamsTable = 'SNOWPARAMS'
snowdatesTable = 'SNOWDATES'
rasweresultsTable = 'RASWERESULTS'
snowinitvarTable = 'SNOWINITVAR'
inputPrecTable = 'INPUT_P'
inputTempTable = 'INPUT_T'
deftlapserateTable = 'DEFTLAPSERATE'
eicorrTable = 'EICORR'
parametersTable = 'PARAMETERS'
snowresultsTable = 'SNOWRESULTS'
snowstatevarTable ='SNOWSTATEVAR'

########################################################################################################################
# acquiring calling arguments
if len(sys.argv) == 1:  # check if the five required arguments were passed. In this case sys.argv has 6 elements, the
    # first is the name of the script, the others the passed arguments. Otherwise, if no arguments are passed sys.argv
    # has only one element, the script name
    # print('len',len(sys.argv), sys.argv[0])  # prints the script name if uncommented
    #odbc = input('Insert ODBC connection: ')
    odbc = 'topmelt'
    #user = input('Insert username: ')
    user = "pantarhei"
    #pw = input('Insert password: ')
    pw = 'arffs'
    #idRun = int(input('Insert Run ID: '))
    idRun = 1
    #logPath = input('Insert log directory: ')
    logPath = './'
else:  # if one or more argument were passed (five are required)
    # print('len',len(sys.argv))
    odbc = sys.argv[1]
    user = sys.argv[2]
    pw = sys.argv[3]
    idRun = sys.argv[4]
    logPath = sys.argv[5]

os.chdir(logPath)  # working directory set
log_file = 'log_run'+str(idRun).zfill(5)+'.txt'
with open(log_file, 'a') as out_file:  # 'w' rewrites
    out_file.write('odbc, user, pw, idrun, logfile path: ' + odbc + ' ' + user + ' ' + pw + ' ' +
                   str(idRun) + ' ' + logPath + '\n')
sys.stdout.write('odbc, user, pw, idrun, logfile path: ' + odbc + ' ' + user + ' ' + pw + ' ' +
               str(idRun) + ' ' + logPath + '\n')

# connect to DB
try:
    cnxn = pyodbc.connect(DSN=odbc, UID=user, PWD=pw)
except pyodbc.Error as odbcerror:
    sys.stdout.write('Connection error: ' + str(odbcerror) + '\n')
    with open(log_file, 'a') as out_file:
        out_file.write('Connection error: ' + str(odbcerror) + '\n')


# reading start and end date, and calculate the number of simulations steps
(dateFrom, dateTo, basinId, paramsetId) = readrun(idRun, cnxn, runsTable, log_file)
if dateTo <= dateFrom:
    with open(log_file, 'a') as out_file:
        out_file.write('ERROR! dtFrom is greater or equal to dtTo' + '\n')
    sys.stdout.write('ERROR! dtFrom is greater or equal to dtTo' + '\n')
    sys.exit(1)
dateFromSim = dateFrom + datetime.timedelta(hours=1)  # simulation starts at second hour
deltaDateSim = dateTo - dateFromSim
simHours = int(deltaDateSim.days * 24 + deltaDateSim.seconds / 3600) + 1

with open(log_file, 'a') as out_file:
    out_file.write('Simulation start and end dates: ' + '\n')
    out_file.write('Start:' + str(dateFrom) + '\n')
    out_file.write('End:' + str(dateTo) + '\n')

########################################################################################################################
#   Acquiring geometry and parameters
########################################################################################################################

with open(log_file, 'a') as out_file:
    out_file.write('Acquiring geometry and parameters: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Acquiring geometry and parameters: ' + str(datetime.datetime.now()) + '\n')

# reads basin area, average quote and raster ID
(basinArea, basinQuote, basinCod) = readbasin(basinId, cnxn, basinsTable, log_file)
# reads number of elevation bands
bandsNum = readbandsnum(basinCod, cnxn, bandsTable, log_file)
# reads band properties (area, elev., ID)
(bandsCod, bandsArea, bandsElevFrom, bandsElevTo) = readbandsprop(basinCod, bandsNum, cnxn, bandsTable, log_file)
# reads maximum number of energy classes
classesNum = readclassesnum(cnxn, classesTable, log_file)
# reads the number of EI maps
nSwitches = readeiswitches(cnxn, eiwitchTable, log_file)
# reads the EI switch dates
(mapsIds, switchDays, switchMonths) = readeiswitchdates(cnxn, eiwitchTable, log_file)
# identify the current EI maps at the start time
mapCur = mapidentification(nSwitches, switchMonths, switchDays, dateFrom.month, dateFrom.day, mapsIds)
# reads classes properties
(classesArea, classesCod, classesGlacArea, classesDebrisArea) = readclassesprop(basinCod, bandsCod, bandsNum,
                                                                    classesNum, mapCur, cnxn, classesTable, log_file)
# reads snow params
(rmf, cmf, erf, albSnow, albglac, beta2, precGrad, nmf, gammah) = readsnowparams(paramsetId, basinCod, cnxn,
                                                                                   snowparamsTable, log_file)
# reads general parameters
(crTemp, baseTemp, liquidW, delayTime, refreezing, avgLatitude, ecf) = readparams(paramsetId, cnxn, parametersTable,
                                                                                  log_file)
# reads energy index of the first map
(eiClass, minEiClass) = readenergyindex(erf, mapCur, basinCod, bandsCod, bandsNum, classesNum, cnxn, classesTable,
                                        log_file)
# reads dates of output snow maps
rasWeDates = readraswedates(idRun, dateFromSim, dateTo, cnxn, snowdatesTable, log_file)

with open(log_file, 'a') as out_file:
    out_file.write('Geometry and parameters acquired: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Geometry and parameters acquired: ' + str(datetime.datetime.now()) + '\n')

########################################################################################################################
#   Acquiring initial conditions
########################################################################################################################

with open(log_file, 'a') as out_file:
    out_file.write('Acquiring initial conditions: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Acquiring initial conditions: ' + str(datetime.datetime.now()) + '\n')

# read snow initial conditions. It is not necessary to have melt as a state variable, because it would only form the
# outflow of an hydrologic model, but it was kept anyway.
(weClass0, weBand0, weAreal0, liqwClass0, liqwBand0, liqwAreal0, iceClass0, iceBand0, iceAreal0, sumTClass0,
 sumTBand0, meltClass0, meltBand0, meltAreal0) = readsnowinitvar(idRun, basinCod, dateFrom, mapCur, bandsCod,
                                                                 bandsArea, classesNum, bandsNum, classesArea,
                                                                 basinArea, classesCod, cnxn, snowinitvarTable,
                                                                 classesTable, log_file)

with open(log_file, 'a') as out_file:
    out_file.write('Initial conditions acquired: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Initial conditions acquired: ' + str(datetime.datetime.now()) + '\n')

########################################################################################################################
#   Acquiring temperature and precipitation (band variables)
########################################################################################################################

with open(log_file, 'a') as out_file:
    out_file.write('Acquiring temperature and precipitation: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Acquiring temperature and precipitation: ' + str(datetime.datetime.now()) + '\n')

(tempBand, precBand) = readmeteoinput(idRun, simHours, dateFromSim, dateTo, basinCod,
                                                               bandsNum, bandsCod, bandsArea, cnxn,
                                                               inputPrecTable, inputTempTable, log_file)
with open(log_file, 'a') as out_file:
    out_file.write('Temperature and precipitation acquired: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Temperature and precipitation acquired: ' + str(datetime.datetime.now()) + '\n')

########################################################################################################################
#   Simulation
########################################################################################################################

with open(log_file, 'a') as out_file:
    out_file.write('Starting simulation: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Starting simulation: ' + str(datetime.datetime.now()) + '\n')

# initializing variables. Areal variables will be stored for each hour and written in tables at the end
weAreal = [0.0 for i in range(simHours)]  # simhours + 1 because we store also init
meltAreal = [0.0 for j in range(simHours)]
iceAreal = [0.0 for k in range(simHours)]
liqwAreal = [0.0 for l in range(simHours)]
snowfallAreal = [0.0 for m in range(simHours)]
rainfallAreal = [0.0 for n in range(simHours)]
dateArray = [None for o in range(simHours)]
baseflowAreal = [0.0 for n in range(simHours)]
baseflowGlacAreal = [0.0 for n in range(simHours)]

# classes variables will not be stored at each time step, but always overwritten
weClass = weClass0
liqwClass = liqwClass0
iceClass = iceClass0
sumTClass = sumTClass0
meltClass = meltClass0  # this is not necessary, in snowmodel it will be set equal to 0

# classes variables will not be stored at each time step, but always overwritten
meltBand = meltBand0
weBand = weBand0
liqwBand = liqwBand0
iceBand = iceBand0
sumTBand = sumTBand0

dateCur = dateFromSim
monthCur = dateCur.month  # current month
(sunset, sunrise) = readsunsetsunrise(dateCur, cnxn, deftlapserateTable, log_file)  # initialize sunset and sunrise
hourCounter = 0  # 0 is not the initial condition date, but the second time step!
while True:
    dateArray[hourCounter] = dateCur
    sys.stdout.write('Simulating ' + str(dateCur) + ' ' + str(hourCounter) + '\n')
    # get sunset and sunrise
    if dateCur.month != monthCur:
        monthCur = dateCur.month
        (sunset, sunrise) = readsunsetsunrise(dateCur, cnxn, deftlapserateTable, log_file)
    # EI maps switch at midnight
    if dateCur.hour == 0:
        mapCurPrec = mapCur
        mapCur = mapidentification(nSwitches, switchMonths, switchDays, dateCur.month, dateCur.day, mapsIds)
        if mapCur != mapCurPrec:  # then it is switch time
            with open(log_file,'a') as out_file:
                out_file.write('Switching from EI map ' + str(mapCurPrec) + ' to map ' + str(mapCur) + '\n')
            sys.stdout.write('Switching from EI map ' + str(mapCurPrec) + ' to map ' + str(mapCur ) + '\n')
            # read the coefficient of corretion from source to destination maps (mapCurPrec to mapCur)
            eiCorrClass = readeicorr(mapCurPrec, basinCod, bandsNum, classesNum, bandsCod, classesCod, cnxn,
                                     eicorrTable, log_file)
            # correct classes variables
            weClass = classvarcorrect(eiCorrClass, weClass, classesNum, bandsNum, bandsArea)
            liqwClass = classvarcorrect(eiCorrClass, liqwClass, classesNum, bandsNum, bandsArea)
            iceClass = classvarcorrect(eiCorrClass, iceClass, classesNum, bandsNum, bandsArea)
            sumTClass = classvarcorrect(eiCorrClass, sumTClass, classesNum, bandsNum, bandsArea)
            meltClass = classvarcorrect(eiCorrClass, meltClass, classesNum, bandsNum, bandsArea)
            # read new classes properties
            (classesArea, classesCod, classesGlacArea, classesDebrisArea) = readclassesprop(basinCod, bandsCod, bandsNum, classesNum,
                                                                         mapCur, cnxn, classesTable, log_file)
            # read new energy indexes
            (eiClass, minEiClass) = readenergyindex(erf, mapCur, basinCod, bandsCod, bandsNum, classesNum, cnxn,
                                                        classesTable, log_file)

    # call snow model
    (weClass, weAfloat, liqwClass, liqwAfloat, iceClass, iceAfloat, meltClass,
     meltAfloat, snowfallAFloat, rainfallAFloat, sumTBand, baseflowAFloat, baseflowGlacAFloat) = \
        snowmodel(albSnow, bandsArea, bandsNum, baseTemp, basinArea, beta2, classesNum, classesArea, classesGlacArea,
              cmf, crTemp, dateCur, delayTime, eiClass, minEiClass, iceClass, liquidW, liqwClass, nmf, albglac, gammah,
              precBand[hourCounter], refreezing, rmf, sumTBand, sunrise, sunset, tempBand[hourCounter], weClass,
                  classesDebrisArea)

    weAreal[hourCounter] = weAfloat
    liqwAreal[hourCounter] = liqwAfloat
    iceAreal[hourCounter] = iceAfloat
    meltAreal[hourCounter] = meltAfloat
    snowfallAreal[hourCounter] = snowfallAFloat
    rainfallAreal[hourCounter] = rainfallAFloat
    baseflowAreal[hourCounter] = baseflowAFloat
    baseflowGlacAreal[hourCounter] = baseflowGlacAFloat

    # call intermediate output
    if rasWeDates is not None:
        for i in range(len(rasWeDates)):
            if dateCur == rasWeDates[i]:
                with open(log_file, 'a') as out_file:
                    out_file.write('Writing snow raster WE results: ' + str(datetime.datetime.now()) + '\n')
                sys.stdout.write('Writing snow raster WE results: ' + str(datetime.datetime.now()) + '\n')
                writerasweresults(bandsArea, bandsCod, bandsNum, classesCod, classesNum, basinCod, idRun, weClass,
                                  liqwClass, meltClass, iceClass, dateCur, cnxn, rasweresultsTable, log_file)

    if dateCur == dateTo:  # cycle end when current date is equal to end date
        break
    dateCur += datetime.timedelta(hours=1)
    hourCounter += 1

with open(log_file, 'a') as out_file:
    out_file.write('Simulation ends: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Simulation ends: ' + str(datetime.datetime.now()) + '\n')

########################################################################################################################
#   Output
########################################################################################################################

with open(log_file, 'a') as out_file:
    out_file.write('Writing snow results: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Writing snow results: ' + str(datetime.datetime.now()) + '\n')
# writes hourly areal results
writesnowresults(simHours, idRun, basinCod, dateArray, weAreal, liqwAreal, iceAreal, meltAreal,
                 snowfallAreal, rainfallAreal,  baseflowAreal, baseflowGlacAreal, cnxn, snowresultsTable, log_file)
# writes final class variable (snowstatevar = snowinitvar)
writesnowstatevar(bandsArea, bandsCod, bandsNum, classesCod, classesNum, basinCod, idRun, weClass,
                              liqwClass, meltClass, iceClass, dateTo, sumTBand, cnxn, snowstatevarTable, log_file)

with open(log_file, 'a') as out_file:
    out_file.write('Snow results written: ' + str(datetime.datetime.now()) + '\n')
sys.stdout.write('Snow results written: ' + str(datetime.datetime.now()) + '\n')

with open(log_file, 'a') as out_file:
    out_file.write('\n')
 
cnxn.close()  # closing connection
