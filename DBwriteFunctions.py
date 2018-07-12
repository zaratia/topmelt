import pyodbc
import sys
import inspect

def writerasweresults(bandsarea, bandscod, bandsnum, classescod, classesnum, basincod, idrun, weclass, liqwclass,
                      meltclass, iceclass, date, cnxn, table, log_file):
    """Writes the info to extract WE rasters"""
    curs = cnxn.cursor()
    input_list = [[None for k in range(9)] for j in range(classesnum)]
    for i in range(bandsnum):
        if bandsarea[i] > 0:
            for j in range(classesnum):
                input_list[j][0] = idrun
                input_list[j][1] = basincod
                input_list[j][2] = bandscod[i]
                input_list[j][3] = classescod[i][j]
                input_list[j][4] = weclass[i][j]
                input_list[j][5] = liqwclass[i][j]
                input_list[j][6] = iceclass[i][j]
                input_list[j][7]= meltclass[i][j]
                input_list[j][8] = date

            sqlstr = 'INSERT INTO ' + table + \
                    ' (IDRUN, IDBASIN, IDELEVBND, IDENERGYBND, WE, LIQW, ICE, MELT, DATE)' \
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
            try:
               curs.executemany(sqlstr, input_list)
            except pyodbc.Error as odbcerror:
               sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
               with open(log_file,'a') as out_file:
                   out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror)
                                  + '\n')
               sys.exit(1)
            curs.commit()
    curs.close()
    return


def writesnowresults(simhours, idrun, basincod, datearray, weareal, liqwareal, iceareal, meltareal,
                     snowareal, rainareal, baseflowareal, baseflowglacareal, cnxn, table, log_file):
    """Writes areal results"""

    input_list = [[None for k in range(11)] for i in range(simhours)]
    curs = cnxn.cursor()

    for i in range(simhours):
        input_list[i][0] = idrun
        input_list[i][1] = basincod
        input_list[i][2] = datearray[i]
        input_list[i][3] = weareal[i]
        input_list[i][4] = liqwareal[i]
        input_list[i][5] = iceareal[i]
        input_list[i][6] = meltareal[i]
        input_list[i][7] = snowareal[i]
        input_list[i][8] = rainareal[i]
        input_list[i][9] = baseflowareal[i]
        input_list[i][10] = baseflowglacareal[i]

    sqlstr = 'INSERT INTO ' + table + ' (IDRUN, IDBASIN, DT, WE, LIQW, ICE, MELT, SNOWFALL, RAINFALL, BASEFLOW, BASEFLOWGLAC) ' \
                                      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    try:
        curs.executemany(sqlstr, input_list)
    except pyodbc.Error as odbcerror:
        sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
        with open(log_file, 'a') as out_file:
            out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror) + '\n')
        sys.exit(1)
    curs.commit()
    curs.close()
    return


def writesnowstatevar(bandsarea, bandscod, bandsnum, classescod, classesnum, basincod, idrun, weclass, liqwclass,
                      meltclass, iceclass, date, sumtband, cnxn, table, log_file):
    """Writes the info to extract WE rasters"""
    curs = cnxn.cursor()
    input_list = [[None for k in range(10)] for j in range(classesnum)]
    for i in range(bandsnum):
        if bandsarea[i] > 0:
            for j in range(classesnum):
                input_list[j][0] = idrun
                input_list[j][1] = basincod
                input_list[j][2] = bandscod[i]
                input_list[j][3] = classescod[i][j]
                input_list[j][4] = weclass[i][j]
                input_list[j][5] = liqwclass[i][j]
                input_list[j][6] = iceclass[i][j]
                input_list[j][7]= meltclass[i][j]
                input_list[j][8] = date
                input_list[j][9] = sumtband[i]

            sqlstr = 'INSERT INTO ' + table + \
                    ' (IDRUN, IDBASIN, IDELEVBND, IDENERGYBND, WE, LIQW, ICE, MELT, ENDDATE, SUMT)' \
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            try:
               curs.executemany(sqlstr, input_list)
            except pyodbc.Error as odbcerror:
               sys.stdout.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror))
               with open(log_file,'a') as out_file:
                   out_file.write('ERROR in function ' + inspect.currentframe().f_code.co_name + str(odbcerror)
                                  + '\n')
               sys.exit(1)
            curs.commit()
    curs.close()
    return