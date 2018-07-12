import pyodbc
import inspect
import sys

def readbandsnum(basincod, cnxn, table, log_file):
    """Reads elevation bands number from table ELEVBNDS"""
    # queries the number of elevation bands of the basin
    sqlstr = 'SELECT COUNT(ID) FROM ' + table + ' WHERE IDBASIN = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, str(basincod))
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    bandsnum = tmp[0][0]
    curs.close()
    return bandsnum


def readbandsprop(basincod, bandsnum, cnxn, table, log_file):
    """Reads elevation bands properties from table ELEVBNDS"""
    # queries area, elevation range and ID of the elevation bands
    sqlstr = 'SELECT ID,AREA,ELEVFROM,ELEVTO FROM ' + table + ' WHERE IDBASIN = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, str(basincod))
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    bandscod = [tmp[i][0] for i in range(bandsnum)]
    bandsarea = [tmp[i][1] for i in range(bandsnum)]
    bandselevfrom = [tmp[i][2] for i in range(bandsnum)]
    bandselevto = [tmp[i][3] for i in range(bandsnum)]
    curs.close()
    return bandscod, bandsarea, bandselevfrom, bandselevto


def readbasin(basinid, cnxn, table, log_file):
    """Reads basin info from table BASINS"""
    # queries basin area, average elevation and ID of the basin in the raster mask

    sqlstr = 'SELECT AREA,AVGQUOTE,IDRASTER FROM ' + table + ' WHERE ID = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, str(basinid))
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                         '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    basinarea = tmp[0][0]
    basinquote = tmp[0][1]
    basincod = tmp[0][2]
    curs.close()
    return basinarea, basinquote, basincod


def readclassesnum(cnxn, table, log_file):
    """Reads maximum number of energy classes"""
    # queries the maximum number of energy classes
    sqlstr = 'SELECT COUNT(ID) FROM ' + table + ' GROUP BY IDBASIN, IDEIMAP, IDELEVBND'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                         '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    classesnum = [tmp[i][0] for i in range(len(tmp))]
    classesmaxnum = max(classesnum)
    curs.close()
    return classesmaxnum


def readclassesprop(basincod, bandscod, bandsnum, classesnum, mapcur, cnxn, table, log_file):
    """Reads the properties of energy classes"""
    # queries the area and the ID codes of energy classes
    classescod = [[0 for j in range(classesnum)] for i in range(bandsnum)]
    classesarea = [[0 for j in range(classesnum)] for i in range(bandsnum)]
    classesglacarea = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]
    classesdebrisarea = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]

    curs = cnxn.cursor()
    for i in range(bandsnum):
        sqlstr = 'SELECT ' + table + '.AREA, ' + table + '.GLACIER_AREA, ' + table + '.DEBRIS_AREA, ' + table +  \
                 '.ID FROM ' + table + ' WHERE ' + table + '.IDBASIN = ? AND ' + table + '.IDELEVBND = ? AND ' + \
                  table + '.IDEIMAP = ? ORDER BY ' + table + '.PERCENTILE'
        try:
            curs.execute(sqlstr, basincod, bandscod[i], mapcur)
        except pyodbc.Error as odbcerror:
            sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
            with open(log_file, 'a') as out_file:
                out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
            sys.exit(1)
        tmp = curs.fetchall()
        if not tmp:
            sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                             '. Query: ' + sqlstr)
            with open(log_file, 'a') as out_file:
                out_file.write(
                    'No rows returned from function ' + inspect.currentframe().f_code.co_name +
                    '. Query: ' + sqlstr + '\n')
            sys.exit(1)
        classesarea[i] = [tmp[j][0] for j in range(classesnum)]
        classesglacarea[i] = [tmp[j][1] for j in range(classesnum)]
        classesdebrisarea[i] = [tmp[j][2] for j in range(classesnum)]
        classescod[i] = [tmp[j][3] for j in range(classesnum)]
    curs.close()
    return classesarea,classescod, classesglacarea, classesdebrisarea


def readeiswitchdates(cnxn, table, log_file):
    """Reads the number of EI maps"""
    # queries maps ID codes, the switch day and the switch month
    sqlstr = 'SELECT ' + table + '.IDEIMAP, ' + table + '.DAY, ' + table + '.MONTH FROM ' + table + ' ORDER BY ' + \
             table + '.IDEIMAP'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                         '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    mapsids = [tmp[i][0] for i in range(len(tmp))]
    switchdays = [tmp[i][1] for i in range(len(tmp))]
    switchmonths = [tmp[i][2] for i in range(len(tmp))]
    curs.close()
    return mapsids, switchdays, switchmonths


def readeicorr(mapcurprec, basincod, bandsnum, classesnum, bandscod, classescod, cnxn, table, log_file):
    """Reads the EI correction coefficients"""
    # queries reaidng the correction coefficient during EI maps migration
    eicorr = [[[-9999 for k in range(classesnum)] for j in range(classesnum)] for i in range(bandsnum)]

    curs = cnxn.cursor()
    for i in range(bandsnum):
        for j in range(classesnum):
            sqlstr = 'SELECT EICORR FROM ' + table + \
                     ' WHERE IDBASIN = ? AND IDELEVBND = ? AND IDENERGYBND = ? AND IDEIMAP = ? ORDER BY PERCTO'
            try:
                curs.execute(sqlstr, basincod, bandscod[i], classescod[i][j], mapcurprec)
            except pyodbc.Error as odbcerror:
                sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
                with open(log_file, 'a') as out_file:
                    out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
                sys.exit(1)
            tmp = curs.fetchall()
            if not tmp:
                sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                                 '. Query: ' + sqlstr)
                with open(log_file, 'a') as out_file:
                    out_file.write(
                        'No rows returned from function ' + inspect.currentframe().f_code.co_name +
                        '. Query: ' + sqlstr + '\n')
                sys.exit(1)
            for k in range(classesnum):
                eicorr[i][k][j] = tmp[k][0]
    curs.close()
    return eicorr

def readeiswitches(cnxn, table, log_file):
    """Reads the number of EI maps"""
    # queries the number of maps that the code will switch through
    sqlstr = 'SELECT COUNT(IDEIMAP) FROM ' + table
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                         '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    nswitches = tmp[0][0]
    curs.close()
    return nswitches


def readenergyindex(erf, mapcur, basincod, bandscod, bandsnum, classesnum, cnxn, table, log_file):
    """Reads the EI from the actual map"""
    # queries the EI, the minimum EI
    classesei = [[0 for j in range(classesnum)] for i in range(bandsnum)]
    classesminei = [[0 for j in range(classesnum)] for i in range(bandsnum)]

    curs = cnxn.cursor()
    for i in range(bandsnum):  # bands
        sqlstr = 'SELECT ' + table + '.AVGENERGY, ' + table + '.MINENERGY FROM ' + table + ' WHERE ' + table + \
                 '.IDBASIN = ? AND ' + table + '.IDELEVBND = ? AND ' + table + '.IDEIMAP = ? ORDER BY ' + \
                 table + '.PERCENTILE'
        try:
            curs.execute(sqlstr, basincod, bandscod[i], mapcur)
        except pyodbc.Error as odbcerror:
            sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
            with open(log_file, 'a') as out_file:
                out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
            sys.exit(1)
        tmp = curs.fetchall()
        if not tmp:
            sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                             '. Query: ' + sqlstr)
            with open(log_file, 'a') as out_file:
                out_file.write(
                    'No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' +
                    sqlstr + '\n')
            sys.exit(1)

        for j in range(classesnum):  # classes

            if tmp[j][0] > 0:  # exclude -999 et al. from correction
                classesei[i][j] = tmp[j][0] * erf
            else:
                classesei[i][j] = tmp[j][0]

            classesminei[i][j] = tmp[j][1]

    curs.close()
    return classesei, classesminei


def readmeteoinput(idrun, simhours, dtfrom, dtto, basincod, bandsnum, bandscod, bandsarea, cnxn,
                   inputprectable, inputtemptable, log_file):
    """Reads precipitation and temperature input"""
    # reads precipitation and temperature for each band, starting from second hour simulation hours shold be equal to
    # number of rows from the queries
    prec = [[-9999.0 for j in range(bandsnum)] for i in range(simhours)]
    temp = [[-9999.0 for j in range(bandsnum)] for i in range(simhours)]

    curs = cnxn.cursor()
    for i in range(bandsnum):
        if bandsarea[i] > 0:
            # get temperature
            sqlstr = 'SELECT TEMPERATURE FROM ' + inputtemptable + \
                     ' WHERE IDRUN = ? AND DT >= ? AND DT <= ? AND IDELEVBND = ? AND IDBASIN = ? ORDER BY DT'
            try:
                curs.execute(sqlstr, idrun, dtfrom, dtto, bandscod[i], basincod)
            except pyodbc.Error as odbcerror:
                sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
                with open(log_file, 'a') as out_file:
                    out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
                sys.exit(1)
            tmp = curs.fetchall()
            if not tmp:
                sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                                 '. Query: ' + sqlstr)
                with open(log_file, 'a') as out_file:
                    out_file.write(
                        'No rows returned from function ' + inspect.currentframe().f_code.co_name +
                        '. Query: ' + sqlstr + '\n')
                sys.exit(1)
            for j in range(simhours):
                temp[j][i] = tmp[j][0]
            # get precipitation
            sqlstr = 'SELECT PRECIPITATION FROM '  + inputprectable + \
                     ' WHERE IDRUN = ? AND DT >= ? AND DT <= ? AND IDELEVBND = ? AND IDBASIN = ? ORDER BY DT'
            try:
                curs.execute(sqlstr, idrun, dtfrom, dtto, bandscod[i], basincod)
            except pyodbc.Error as odbcerror:
                sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
                with open(log_file, 'a') as out_file:
                    out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
                sys.exit(1)
            tmp = curs.fetchall()
            if not tmp:
                sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                                 '. Query: ' + sqlstr)
                with open(log_file, 'a') as out_file:
                    out_file.write(
                        'No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' +
                        sqlstr + '\n')
                sys.exit(1)
            for j in range(simhours):
                prec[j][i] = tmp[j][0]

    curs.close()
    return temp, prec


def readparams(paramsetid, cnxn, table, log_file):
    """Reads genera parameters"""
    # queries all the general parameters
    sqlstr = 'SELECT CRITICALTEMP, BASETEMP, LIQUIDWATER, DELAYTIME, REFREEZING, AVGLATITUDE, ECF FROM ' + table + \
             ' WHERE IDPARAMSET = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, paramsetid)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' +
                           sqlstr + '\n')
        sys.exit(1)
    crtemp = tmp[0][0]
    basetemp = tmp[0][1]
    liquidw = tmp[0][2]
    delaytime = tmp[0][3]
    refreezing = tmp[0][4]
    avglatitude = tmp[0][5]
    ecf = tmp[0][6]
    curs.close()
    return crtemp, basetemp, liquidw, delaytime, refreezing, avglatitude, ecf


def readraswedates(idrun, dtfrom, dtto, cnxn,table, log_file):
    """Reads the selected dates of snow maps"""
    # reads the selected dates at which the code will write state snow variables
    sqlstr = 'SELECT ' + table + '.DT FROM ' + table + ' WHERE DT >= ? AND DT <= ? AND IDRUN = ? ORDER BY DT'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, dtfrom, dtto, idrun)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        with open(log_file, 'a') as out_file:
            out_file.write('WARNING! No snow output dates selected. \n')
        sys.stdout.write('WARNING! No snow output dates selected. \n')
        raswedates = None
    else:
        raswedates = [tmp[i][0] for i in range(len(tmp))]
    curs.close()
    return raswedates


def readrun(idrun, cnxn, table, log_file):
    """Reads run info from table RUNS"""
    # queries start and end date of the simulation
    sqlstr = 'SELECT DTFROM, DTTO, IDBASIN, IDPARAMSET FROM ' + table + ' WHERE ID = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, idrun)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' +
                         sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' +
                           sqlstr + '\n')
        sys.exit(1)
    datefrom = tmp[0][0]
    dateto = tmp[0][1]
    basinid = tmp[0][2]
    paramsetid = tmp[0][3]
    curs.close()
    return datefrom, dateto, basinid, paramsetid


def readsnowinitvar(idrun, basincod, datein, mapcur, bandscod, bandsarea, classesnum, bandsnum, classesarea, basinarea,
                    classescod, cnxn, tablesnow, tableen, log_file):
    """Reads snow initial conditions"""
    # queries water equivalent, ice, liquid water, melt water, and sumT (sumT is a band variable because it depends on
    # temperature, which is constant over bands, but it is treated as a class variable). Be sure that the number of
    # bands in the table is consistent.
    we = [[-9999.0 for j in range(classesnum)] for i in range(bandsnum)]
    liqw = [[-9999.0 for j in range(classesnum)] for i in range(bandsnum)]
    ice = [[-9999.0 for j in range(classesnum)] for i in range(bandsnum)]
    sumt = [[-9999.0 for j in range(classesnum)] for i in range(bandsnum)]
    melt = [[-9999.0 for j in range(classesnum)] for i in range(bandsnum)]
    wetmp = [-9999.0 for i in range(classesnum)]
    liqwtmp = [-9999.0 for i in range(classesnum)]
    icetmp = [-9999.0 for i in range(classesnum)]
    sumttmp = [-9999.0 for i in range(classesnum)]
    melttmp = [-9999.0 for i in range(classesnum)]
    readclasses = [-9999 for i in range(classesnum)]

    curs = cnxn.cursor()
    for i in range(bandsnum):  # read only for bands with area
        if bandsarea[i]>0:

            sqlstr = 'SELECT ' + tablesnow + '.IDENERGYBND, ' + tablesnow + '.WE, ' + tablesnow + '.LIQW, ' + tablesnow + \
                     '.ICE, ' + tablesnow + '.SUMT, ' + tablesnow + '.MELT FROM ' + tablesnow + ', ' + tableen + ' WHERE ' + \
                     tablesnow + '.IDRUN = ? AND ' + tablesnow + '.STARTDATE = ? AND ' + tableen + '.IDEIMAP = ? AND ' + \
                     tablesnow + '.IDELEVBND = ? AND ' + tablesnow + '.IDELEVBND = ' + tableen + '.IDELEVBND AND ' + \
                     tablesnow + '.IDBASIN = ? ORDER BY ' + tableen + '.PERCENTILE'
            try:
                curs.execute(sqlstr, idrun, datein, mapcur, bandscod[i], basincod)
            except pyodbc.Error as odbcerror:  # error returned from query
                sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
                with open(log_file, 'a') as out_file:
                    out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
                sys.exit(1)
            tmp = curs.fetchall()
            if not tmp:
                sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                                 '. Query: ' + sqlstr + '. Initital conditions set to 0.\n')
                with open(log_file, 'a') as out_file:
                    out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                                   '. Query: ' + sqlstr + '. Initital conditions set to 0.\n')
                for j in range(classesnum):
                    we[i][j] = 0.0
                    liqw[i][j] = 0.0
                    ice[i][j] = 0.0
                    sumt[i][j] = 0.0
                    melt[i][j] = 0.0
                #sys.exit(1)
            else:
                for j in range(classesnum):
                    readclasses[j] = tmp[j][0]
                    wetmp[j] = tmp[j][1]
                    liqwtmp[j] = tmp[j][2]
                    icetmp[j] = tmp[j][3]
                    sumttmp[j] = tmp[j][4]
                    melttmp[j] = tmp[j][5]

                for clas in range(classesnum):
                    for clas2 in range(classesnum):
                        if classescod[i][clas] == readclasses[clas2]:
                            we[i][clas] = wetmp[clas2]
                            liqw[i][clas] = liqwtmp[clas2]
                            ice[i][clas] = icetmp[clas2]
                            melt[i][clas] = melttmp[clas2]
                            sumt[i][clas] = sumttmp[clas2]

    # initializing band variables
    web = [0.0 for i in range(bandsnum)]
    liqwb = [0.0 for i in range(bandsnum)]
    iceb = [0.0 for i in range(bandsnum)]
    sumtb = [0.0 for i in range(bandsnum)]
    meltb = [0.0 for i in range(bandsnum)]
    wea = 0.0
    liqwa = 0.0
    icea = 0.0
    melta = 0.0
    for i in range(bandsnum):
        for j in range(classesnum):
            if classesarea[i][j] >0:
                web[i] = web[i] + we[i][j] * classesarea[i][j] / bandsarea[i]
                liqwb[i] = liqwb[i] + liqw[i][j] * classesarea[i][j] / bandsarea[i]
                iceb[i] = iceb[i] + ice[i][j] * classesarea[i][j] / bandsarea[i]
                sumtb[i] = sumtb[i] + sumt[i][j] * classesarea[i][j] / bandsarea[i]
                meltb[i] = meltb[i] + melt[i][j] * classesarea[i][j] / bandsarea[i]
            else:
                web[i] = -9999.0
                liqwb[i] = -9999.0
                iceb[i] = -9999.0
                sumtb[i] = -9999.0
                meltb[i] = -9999.0
        if bandsarea[i] > 0:
            wea += web[i] * bandsarea[i] / basinarea
            liqwa += liqwb[i] * bandsarea[i] / basinarea
            icea += iceb[i] * bandsarea[i] / basinarea
            melta += meltb[i] * bandsarea[i] / basinarea

    curs.close()
    return we, web, wea, liqw, liqwb, liqwa, ice, iceb, icea, sumt, sumtb, melt, meltb, melta


def readsnowmapsnum(idrun, dtfrom, dtto, cnxn, table, log_file):
    """Counts the selected dates of snow maps"""
    # counts the selected dates at which the code will write state snow variables
    sqlstr = 'SELECT COUNT(' + table + '.ID) FROM ' + table + ' WHERE DT >= ? AND DT <= ? AND IDRUN = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, dtfrom, dtto, idrun)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                         '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: ' +
                           sqlstr + '\n')
        sys.exit(1)
    wemapsnum = tmp[0][0]
    curs.close()
    return wemapsnum


def readsnowparams(paramsetid, basincod, cnxn, table, log_file):
    """Reads snow parameters"""
    # queries all the snow parameters of the basin
    sqlstr = 'SELECT RMF, CMFMIN/2 + CMFMAX/2, ERF, ALBSNOW, ALBGLAC, BETA2, PRECGRAD, NMF, GAMMAH FROM ' + table + \
             ' WHERE IDPARAMSET = ? AND IDBASIN = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, paramsetid, basincod)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                         '. Query: ' + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name +
                           '. Query: ' + sqlstr + '\n')
        sys.exit(1)
    rmf = tmp[0][0]
    cmf = tmp[0][1]
    erf = tmp[0][2]
    albsnow = tmp[0][3]
    albglac = tmp[0][4]
    beta2 = tmp[0][5]
    precgrad = tmp[0][6]
    nmf = tmp[0][7]
    gammah = tmp[0][8]
    curs.close()
    return rmf, cmf, erf, albsnow, albglac, beta2, precgrad, nmf, gammah


def readsunsetsunrise(datecur, cnxn, table, log_file):
    """Reads sunset and sunrise"""
    # queries sunrise and sunset dates from the relative table
    sqlstr = 'SELECT SUNSET, SUNRISE FROM ' + table + ' WHERE IDMONTH = ?'
    curs = cnxn.cursor()
    try:
        curs.execute(sqlstr, datecur.month)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    tmp = curs.fetchall()
    if not tmp:
        sys.stdout.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: '
                         + sqlstr)
        with open(log_file, 'a') as out_file:
            out_file.write('No rows returned from function ' + inspect.currentframe().f_code.co_name + '. Query: '
                           + sqlstr + '\n')
        sys.exit(1)
    sunset = tmp[0][0]
    sunrise = tmp[0][1]
    curs.close()
    return sunset, sunrise