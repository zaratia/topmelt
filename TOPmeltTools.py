def classvarcorrect(eicorr, classvar, classesnum, bandsnum, bandsarea):
    """Corrects a class variable"""
    # Corrects a class variable according to the coefficients of correction
    for i in range(bandsnum):
        if bandsarea[i] > 0:
            for j in range(classesnum):  # destination class
                vartmp = 0
                for k in range(classesnum):  # destination class
                    vartmp += classvar[i][k] * eicorr[i][j][k]
                classvar[i][j] = vartmp
    return classvar


def mapidentification(nswitches, swmonths, swdays, monthcur, day, mapsids):
    """Returns the Id of the actual EI map"""
    # monthcur is the current month

    mapcur = None
    switched = 0
    for i in range(nswitches):
        # if the date month is less than the kk-th switch date month than we still are in the kk-th map, or
        # if the date month is equal to the kk-th switch month than if the day is less than the kk-th switch day we
        # are in the kk-th map.
        if switched == 0:  # stop when EI is switched
            if monthcur < swmonths[i] or (monthcur == swmonths[i] and day < swdays[i]):
                mapcur = mapsids[i]
                switched = 1  # switched is true

    if switched == 0:
        # if no maps is found than we are in the first again because the date is higher or equal
        # to any switch date
        mapcur = mapsids[0]

    return mapcur  # returns actual map
