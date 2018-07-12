import math

def snowmodel(albsnow, bandsarea, bandsnum, basetemp, basinarea, beta2, classesnum, classesarea, classesglacarea,
              cmf, crtemp, datecur, delaytime, ei, eimin, iceclass, liquidwater,
              liqwclass, nmf, albglac, gammah, precband, refreezing, rmf, sumtband, sunrise,
              sunset, tempband, weclass, classesdebrisarea):
    """Snow model"""
    # Estimate snow accumulation and melting processes. precband and tempband are band input data.

    # class variables init
    snowclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]
    rainclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]
    rainsnowclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]  # rain falling on snow
    fusionclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]
    storedvolclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]  # stored volume
    excessclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]  # excess water
    meltclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]  # excess water
    #
    fusionglacclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]
    weglacmeltclass = [[0.0 for j in range(classesnum)] for i in range(bandsnum)]  # this is baseflowglacclass

    # band variables init
    snowband = [0.0 for i in range(bandsnum)]
    rainband = [0.0 for i in range(bandsnum)]
    weband = [0.0 for i in range(bandsnum)]
    liqwband = [0.0 for i in range(bandsnum)]
    meltband = [0.0 for i in range(bandsnum)]
    iceband = [0.0 for i in range(bandsnum)]
    #
    baseflowband = [0.0 for i in range(bandsnum)]
    baseflowglacband = [0.0 for i in range(bandsnum)]

    # areal varaible init
    excessareal = 0.0
    snowareal = 0.0
    rainareal = 0.0
    rainsnowareal = 0.0
    liqwareal = 0.0
    weareal = 0.0
    iceareal = 0.0
    meltareal = 0.0
    #
    baseflowareal = 0.0
    # barerainareal = 0.0
    baseflowglacareal = 0.0
    fusionareal = 0.0
    fusionglacareal = 0.0

    itisday =  datecur.hour <= sunset and datecur.hour >= sunrise  # is it day or night?
    dt = 1.0  # parameter for delay time
    for i in range(bandsnum):

        if tempband[i] > 0 and sumtband[i] < 30000:  # 30000 is an arbitrary threshold to sumt growth
            sumtband[i] += tempband[i]

        for j in range(classesnum):
            # if class area is greater than 0
            if classesarea[i][j] > 0:
                # define the index for day and night
                if datecur.hour >= sunrise and datecur.hour <= sunset:
                    eindex = ei[i][j] / (sunset - sunrise) * 24
                else:
                    eindex = eimin[i][j]
                # if it rains
                if precband[i] > 0:
                    # if it is snow
                    if tempband[i] < crtemp:
                        snowclass[i][j] = precband[i]
                        snowband[i] +=  precband[i] * classesarea[i][j] / bandsarea[i]
                        snowareal += precband[i] * classesarea[i][j] / basinarea
                        weclass[i][j] = weclass[i][j] + precband[i]
                    else:  # it rains
                        rainclass[i][j] = precband[i]
                        rainband[i] += precband[i] * classesarea[i][j] / bandsarea[i]
                        rainareal += precband[i] * classesarea[i][j] / basinarea
                        # if there is snow on the ground it is rain on snow
                        if weclass[i][j] > 0:
                            rainsnowareal += precband[i] * classesarea[i][j] / basinarea
                            rainsnowclass[i][j] = precband[i]
                        else:  # if no WE it generates flow
                            baseflowareal += precband[i] * classesarea[i][j] / basinarea
                            baseflowband[i] += precband[i] * classesarea[i][j] / bandsarea[i]
                # albedo estimation
                if snowclass[i][j] > 0.2:
                    sumtband[i] = 0.0
                    albedo = albsnow
                elif sumtband[i] <= 0.1:  # if it is not snowing but sumT is close to zero
                    albedo = albsnow
                else: # no snow falls
                    albedo = (albsnow - beta2) - beta2 * math.log10(sumtband[i])
                # 0.3 is a minimum threshold for albedo
                if albedo < 0.3: albedo = 0.3
                # glacier melting
                if weclass[i][j] <= 0:
                    if tempband[i] > basetemp and classesglacarea[i][j] > 0:
                        if rainband[i] < 0.2:  # a little rain
                            if itisday:  # morphoenergetic fusion
                                fusionglacclass[i][j] = cmf * eindex * (1.0 - albglac) * tempband[i] * \
                                            (1 - classesdebrisarea[i][j] / classesglacarea[i][j] * (math.exp(-gammah)))
                            else:  # night
                                fusionglacclass[i][j] = nmf * tempband[i]
                        else:
                            fusionglacclass[i][j] = (rmf + rainband[i] / 80.0) * tempband[i]
                        baseflowglacareal += fusionglacclass[i][j] * classesglacarea[i][j] / basinarea
                        baseflowglacband[i] += fusionglacclass[i][j] * classesglacarea[i][j] / bandsarea[i]
                        weglacmeltclass[i][j] += fusionglacclass[i][j] * classesglacarea[i][j] / classesarea[i][j]
                    else:
                        fusionglacclass[i][j] = 0.0
                # if there is snow...
                if weclass[i][j] > 0:
                    # ...and it is melting...
                    if tempband[i] > basetemp:
                        # ... and eventually a little rain...
                        # if apreccorr < 0.2:
                        if precband[i] < 0.2:
                            # ...and it is day
                            if itisday:  # morphoenergetic fusion
                                fusionclass[i][j] = cmf * eindex * (1.0 - albedo) * tempband[i]
                            else:  # night
                                fusionclass[i][j] = nmf * tempband[i]
                        else:  # if rain is higher the fusion is from rain
                            fusionclass[i][j] = (rmf + precband[i] / 80.0) * tempband[i]
                        # updating water equivalent
                        if weclass[i][j] >= fusionclass[i][j]:
                            weclass[i][j] += -fusionclass[i][j]
                            fusionglacclass[i][j] = 0.0
                        else:
                            fusionglacclass[i][j] = fusionclass[i][j] - weclass[i][j]
                            baseflowglacareal += fusionglacclass[i][j] * classesglacarea[i][j] / basinarea
                            baseflowglacband[i] += fusionglacclass[i][j] * classesglacarea[i][j] / bandsarea[i]
                            weglacmeltclass[i][j] += fusionglacclass[i][j] * classesglacarea[i][j] / classesarea[i][j]
                            fusionclass[i][j] = weclass[i][j]
                            weclass[i][j] = 0.0
                        # update liquid water
                        liqwmax = weclass[i][j] * liquidwater
                        liqwclass[i][j] += fusionclass[i][j] + rainsnowclass[i][j]

                        if liqwclass[i][j] > liqwmax:  # if liquid water exceeds the threshold
                            excessclass[i][j] = liqwclass[i][j] - liqwmax
                            liqwclass[i][j] = liqwmax
                            # estimate the delay to reach ground, it depends on WE depth
                            late = int(delaytime * weclass[i][j] / 1000)

                            if late > 1:
                                storedvolclass[i][j] = excessclass[i][j] * math.exp(-dt / late)
                                baseflowareal += excessclass[i][j] * classesarea[i][j] / basinarea - \
                                                 storedvolclass[i][j] * classesarea[i][j] / basinarea
                                baseflowband[i] += excessclass[i][j] * classesarea[i][j] / bandsarea[i] - \
                                                   storedvolclass[i][j] * classesarea[i][j] / bandsarea[i]
                                liqwclass[i][j] += storedvolclass[i][j]
                                excessareal += excessclass[i][j] * classesarea[i][j] / basinarea - \
                                               storedvolclass[i][j] * classesarea[i][j] / basinarea
                            else:
                                storedvolclass[i][j] = 0.0
                                baseflowareal += excessclass[i][j] * classesarea[i][j] / basinarea
                                baseflowband[i] = excessclass[i][j] * classesarea[i][j] / bandsarea[i]
                                excessclass[i][j] = 0
                                excessareal += excessclass[i][j] * classesarea[i][j] / basinarea
                                # liqwclass[i][j] = liqwclass[i][j]
                        # updating melt variable
                        meltclass[i][j] += fusionclass[i][j]
                        meltband[i] += fusionclass[i][j] * classesarea[i][j] / bandsarea[i]
                        meltareal += fusionclass[i][j] * classesarea[i][j] / basinarea
                    else:  # tempband < basetemp, no fusion but refreezing
                        fusionclass[i][j] = 0.0
                        fusionglacclass[i][j] = 0.0
                        iceclass[i][j] = refreezing * (basetemp - tempband[i])
                        if liqwclass[i][j] > iceclass[i][j]:  # there is water enough to refreeze
                            liqwclass[i][j] = liqwclass[i][j] - iceclass[i][j]
                        else:  # water is not enough. it all freeze
                            iceclass[i][j] = liqwclass[i][j]
                            liqwclass[i][j] = 0.0
                        # updating ice variable
                        iceareal += iceclass[i][j] * classesarea[i][j] / basinarea
                        iceband[i] += iceclass[i][j] * classesarea[i][j] / bandsarea[i]
                        weclass[i][j] += iceclass[i][j]

                liqwareal += liqwclass[i][j] * classesarea[i][j] / basinarea
                weareal += weclass[i][j] * classesarea[i][j] / basinarea
                fusionareal += fusionclass[i][j] * classesarea[i][j] / basinarea
                fusionglacareal += fusionglacclass[i][j] * classesarea[i][j] / basinarea

                liqwband[i] += liqwclass[i][j] * classesarea[i][j] / bandsarea[i]
                weband[i] += weclass[i][j] * classesarea[i][j] / bandsarea[i]

            else:
                liqwclass[i][j] = -9999.0
                weclass[i][j] = -9999.0
                iceclass[i][j] = -9999.0
                meltclass[i][j] = -9999.0
                snowclass[i][j] = -9999.0
                fusionclass[i][j] = -9999.0
                fusionglacclass[i][j] = -9999.0

    return weclass, weareal, liqwclass, liqwareal, iceclass, iceareal, meltclass, meltareal, snowareal, rainareal, \
           sumtband, baseflowareal, baseflowglacareal
