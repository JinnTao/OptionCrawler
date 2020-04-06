import datetime as dt
import pandas as pd
from sklearn import linear_model as LM

badDays = [
    20140116,20140117,
    20140220,20140221,
    20140320,20140321,
    20140417,20140418,
    20140515,20140516,
    20140619,20140620,
    20140717,20140718,
    20140814,20140815,
    20140918,20140919,
    20141016,20141017,
    20141120,20141121,
    20141218,20141219,    
    20150115,20150116,
    20150219,20150220,
    20150319,20150320,
    20150416,20150417,
    20150514,20150515,20150518,
    20150618,20150619,20150623,
    20150716,20150717,
    20150820,20150821,
    20150917,20150918,
    20151016,20151017,
    20151119,20151120,
]


################################################################################################
##

def createSequentialFolder( prefix = 'Model' ):
    '''
    Description: create sequentially new folder with designated prefix
    Flexible:    YES
    Finalized:   YES
    '''
    import glob, os
    a = glob.glob( prefix+'*' )

    if a:
        a = [ int(i.replace( prefix, '', 1 )) for i in a ]
        a.sort()
        folderName = prefix + str( a[-1] + 1 ) + '/'
    else:
        folderName = prefix + '0/'

    os.mkdir( folderName )
    return folderName

##
################################################################################################
##

def getSignalName( fileName ):
    '''
    Description: Convert .desc file to row names
    Flexible:    NO
    Finalized:   YES
    '''
    signalNames = []
    with open( fileName ) as f:
        [ signalNames.append( item.split( ' ', 1 ) [1].strip() ) for item in f.readlines() ]
    return signalNames 

##
################################################################################################
##

def getSignalDataFrame( dataFile, descFile, index_col='scheduled_timeval'):
    '''
    Description: Convert dataFile to DataFrame, with row names fetched from descFile
    Flexible:    NO
    Finalized:   NO
    Issues:
    1. check whether dataFile is empty
    2. check whether dataFile.col = descFile.row
    '''
    data = pd.read_table( dataFile, sep = ' +', 
                          header = None, 
                          names = getSignalName( descFile ), 
                          index_col = index_col, 
                          parse_dates = index_col, 
                          date_parser = lambda x: dt.datetime.fromtimestamp(float(x)), 
                          keep_date_col = True )
    return data

##
################################################################################################
##

def loadDataParallel( files, descFile, index_col, njobs = 10 ):
    '''
    Description: parallelize the getSignalDataFrame function
    Flexible:    NO
    Finalized:   YES
    '''
    import multiprocessing as mp
    pool = mp.Pool( njobs )
    loadf = getSignalDataFrame
    resultList = [ pool.apply_async( loadf, ( f, descFile, index_col ) ) for f in files ]
    pool.close()
    pool.join()
    results = [ r.get() for r in resultList ]
    return results

##
################################################################################################
##

def mergeDataFrame( X, Y ):
    '''
    Description: Merge two DataFrames using the index of both.
    Flexible:    YES
    Finalized:   YES
    '''
    return X.merge( Y, left_index = True, right_index = True )

##
################################################################################################
##

def sliceTime( X, startTime = dt.time( 9, 15, 0 ), endTime = dt.time( 15, 0, 0 ) ):
    '''
    Description: Slice DatetimeIndexed DataFrame with time window [startTime, endTime]
    Flexible:    YES
    Finalized:   YES
    '''
    return X.ix[ startTime:endTime ]
    
##
################################################################################################
##

def sliceMultiTime( X, windLists ):
    '''
    Description: Slice DatetimeIndexed DataFrame with time window list [ [startTime1, endTime1], ...]
    Flexible:    YES
    Finalized:   YES
    '''
    import pandas as pd
    return pd.concat( [ sliceTime( X, item[0], item[1] ) for item in windLists ])

##
################################################################################################
##

def getTradingDays( startDay, endDay, mktTag = "MKT_SHFE"):
    '''
    Description: Mask of longbeach_pnl.trading_days
    Flexible:    YES
    Finalized:   YES
    '''
    import longbeach_pnl
    return longbeach_pnl.trading_days( start = startDay, end = endDay, mkt = mktTag)

##
################################################################################################
##

def rmBadDays( dateList, badDays ):
    '''
    Description: filter out bad days from date list
    Flexible:    NO
    Finalized:   NO
    Issues:
    1. current badDays and dateList are of type list of float, should be compatible with list of string for later upgrade
    '''
    goodDates = filter( None, [ i if not badDays.count( i ) else None for i in dateList ] )
    return goodDates

##
################################################################################################
##

def _signalgenWorker( d, modelFile ):
    '''
    Description: CLOSURE OF GETSIGNALONDATELIST, DO NOT USE ALONE
    Flexible:    NO
    Finalized:   YES
    '''
    import sys
    import subprocess as sp
    cmd = [ 'signalgen', '-s', str(d), '-e', str(d), '-S', modelFile, '--ignore-missing', '--ovargen' ]
    # print(d)
    try: 
        a = sp.check_output( " ".join( cmd ), shell = True )
        a = None
    except sp.CalledProcessError as e:
        print >> sys.stderr, " ".join( cmd ), "returned", e.returncode, e.output

################################################################################################

def getSignalOnDateList( dateList, modelFile = 'model.lua', njobs = 10 ):
    '''
    Description: run signalgen to get data files on datelist
    Flexible:    NO
    Finalized:   YES
    '''
    import multiprocessing as mp
    pool = mp.Pool( njobs )
    resultList = [ pool.apply_async( _signalgenWorker, ( d, modelFile ) ) for d in dateList ]
    pool.close()
    pool.join()

##
################################################################################################
##

def subsample( fileName, prefix, count, fileMode = 'w', randomSample = True ):
    '''
    Description: subsample rows from fileName to reduce the size
    Flexible:    YES
    Finalized:   YES
    '''
    import random
    stream = open( fileName, 'r' ) 
    source = range(count)
    out = [ open( prefix+'.'+str(i), fileMode ) for i in range( count ) ]
    i = 0
    a = list( source )
    if randomSample :
        for line in stream:
            if i == 0:
                random.shuffle(a)
            
            idx = a[i]
            out[idx].writelines(line)
            i = (i + 1) % len(out)
    else:
        for line in stream:
            out[a[i]].writelines( line ) 
            i = (i + 1) % len(out)

    print(stream)

##
################################################################################################
##

def foldData( nFold = 10, xPrefix = 'trainX', yName = 'trainY.dat', randomSample = True ):
    '''
    Description: fold all the signalgen data with subsample
    Flexible:   NO
    Finalized:  YES
    '''
    import glob
    from os import path
    import subprocess as sp
    isTrigger = True
    a = glob.glob('signalgen.????????.trigger.dat')
    if a == []:
        a = glob.glob('signalgen.????????.ov.dat')
        isTrigger = False
    a.sort()
    ## with the empty dates removed
    b = filter( None, [ isis.split( '.' )[1] if path.getsize( isis ) else None for isis in a] )

    if isTrigger :
        trigAppd = '.trigger.dat'
        ovarAppd = '.ov.trigger.dat'
    else:
        trigAppd = '.dat'
        ovarAppd = '.ov.dat'
    
    if nFold > 1 :
        trigname = 'signalgen.'+b[0]+trigAppd
        ovarname = 'signalgen.'+b[0]+ovarAppd
        subsample( trigname, xPrefix, nFold, fileMode = 'w', randomSample = randomSample )
        sp.call( ' '.join( [ 'cat',ovarname, '>', yName ] ), shell = True  )
        for fitem in b[1:]:
            trigname = 'signalgen.'+fitem+trigAppd
            ovarname = 'signalgen.'+fitem+ovarAppd
            subsample( trigname, xPrefix, nFold, fileMode = 'a', randomSample = randomSample )
            sp.call( ' '.join( [ 'cat',ovarname, '>>', yName ] ), shell = True )
    elif nFold == 1:
        xName = xPrefix + '.0'
        trigname = 'signalgen.'+b[0]+trigAppd
        ovarname = 'signalgen.'+b[0]+ovarAppd
        sp.call( ' '.join( [ 'cat',trigname, '>', xName ] ), shell = True  )
        sp.call( ' '.join( [ 'cat',ovarname, '>', yName ] ), shell = True  )
        for fitem in b[1:]:
            trigname = 'signalgen.'+fitem+trigAppd
            ovarname = 'signalgen.'+fitem+ovarAppd
            sp.call( ' '.join( [ 'cat',trigname, '>>', xName ] ), shell = True )
            sp.call( ' '.join( [ 'cat',ovarname, '>>', yName ] ), shell = True )
    else:
        raise Exception( ' DataAnalysis.foldData: nFold must be positive! ')
        
##
################################################################################################
##

def winsorizeData( dataFrame, targCol, limits = ( 0.05, 0.05 ) ):
    '''
    Description: Mask for scipy winsorize function
    Flexible:    YES
    Finalized:   NO
    Issue:
    1. generalized to the case of clip by standard deviation
    2. generalized to the case of rolling windows using pandas.moments.rolling_std
    '''
    from scipy.stats import mstats
    dataFrame.loc[ :, targCol ] =  mstats.winsorize( dataFrame.loc[ :, targCol ], limits ) 
    return dataFrame

##
################################################################################################
##

def winsorizeDataByDate( dataFrame, targCol, limits = ( 0.05, 0.05 ) ):
    '''
    Description: Winsorize the Data on daily basis
    Flexible:    YES
    Finalized:   NO
    Issue:
    '''
    from scipy.stats import mstats
    dataFrame.loc[ :, 'TMP_DATE' ] = dataFrame.index.date
    dList = dataFrame.TMP_DATE.unique()
    def _workLabor( _date ):
        _adf = dataFrame.loc[ dataFrame.TMP_DATE == _date, : ] 
        _tmpDF = winsorizeData( _adf, targCol, limits )
        dataFrame.loc[ dataFrame.TMP_DATE == _date, : ] = _tmpDF
    [ _workLabor( item ) for item in dList ]
    dataFrame.drop( 'TMP_DATE', axis = 1, inplace = False )    
    return dataFrame
            
##
################################################################################################
##

def writeLinearRegWgtToLua( lsModel,
                            paramNames, 
                            output = 'weight.lua',
                            targName = 'target_decay_rate_0.970' ):
    '''
    Description: write the model coef with intercept from sklearn linear regression model, with term names passed in from paramNames
    Flexible:    NO
    Finalized:   YES
    '''
    import re
    import pandas as pd
    from defaultOrderedDict import DefaultOrderedDict
    import pyObjToLuaTab
    weights = {}

    weights[ 'target_column' ] = targName

    weights[ 'constantWeight' ] = lsModel.intercept_

    paramList = pd.DataFrame( lsModel.coef_, paramNames )
    signals = []
    tree = DefaultOrderedDict( lambda: DefaultOrderedDict( list ) )
    for _, value in paramList.iterrows():
        _t1, _t2 = value.name.split(' ')
        tree[_t1][_t2] = value.item()

    for item in tree.keys():
        _tp = {}
        _tp[ 'desc' ] = item
        _tp[ 'state_names' ] = tree[ item ].keys()
        _tp[ 'weights' ] = tree[ item ].values()
        signals.append( _tp )
    
    weights['signals'] = signals
    with open( output, 'w+' ) as f:
        print >> f, re.sub( '^{\n  ', 'weights = {\n ', pyObjToLuaTab.to_lua( weights ) )

##
################################################################################################
##

def regressionModel( xPrefix, 
                     xDesc,
                     yName,
                     yDesc,
                     timeWindows,
                     index_col = 'scheduled_timeval',
                     targCol = 'target_decay_rate_0.970',
                     clipLimits = ( 0.05, 0.05 ),
                     # fit_intercept = False,
                     # startTime = dt.time( 9, 15, 0 ), 
                     # endTime = dt.time( 15, 0, 0 ),
                     winsorizeByDay = True,
                     func = LM.LassoLars( alpha = 0.05, fit_intercept = False ) ):
    '''
    Description: Linear Regression model building run through
    Flexible:    YES, please check the parameter list
    Finalized:   YES
    '''
    import glob
    import numpy as np
    from copy import deepcopy 
    from sklearn.cross_validation import LeaveOneOut as LOO
    from scipy.stats.stats import pearsonr
    xFileList = glob.glob( xPrefix+'.*' ) 
    nn = len( xFileList )
    xData = loadDataParallel( xFileList, xDesc, index_col, njobs = nn )
    xColName = getSignalName( xDesc )[5:]
    yData = getSignalDataFrame( yName, yDesc, index_col )
    #  yData = sliceTime( yData, startTime, endTime )
    yData = sliceMultiTime( yData, timeWindows )
    if winsorizeByDay :
        yData = winsorizeDataByDate( yData, targCol, limits = clipLimits )
    else:
        yData = winsorizeData( yData, targCol, limits = clipLimits )
    zData = [0]*nn
    for i in range( nn ):
        zData[i] = mergeDataFrame( xData[i], yData)
        ## zData[i] = sliceTime( zData[i], startTime, endTime )
    zData = np.array( zData )

    ## variables to be output
    train_scores = []
    test_scores = []
    train_pearson = []
    test_pearson = []
    coefs = []
    mods = []

    for test, train in LOO( nn ):
        print 'the Training and Test sets are:', train, test
        
        zTrain = pd.concat( zData[ train ].tolist() )
        zTest  = pd.concat( zData[  test ].tolist() )
        xTrain = zTrain[ xColName ]
        yTrain = zTrain[  targCol ]
        xTest  = zTest[  xColName ]
        yTest  = zTest[   targCol ]

        print 'Train set X has the shape :', xTrain.shape
        print 'Train set Y has the shape :', yTrain.shape
        print 'Test set X has the shape :', xTest.shape
        print 'Test set Y has the shape :', yTest.shape

        #  func = LM.LinearRegression( fit_intercept = fit_intercept )
        #  print( ' Bayesian Ridge Regression ' )
        #  func = LM.BayesianRidge( fit_intercept = fit_intercept )
        #  print( ' Lasso Regression ' )
        #  func = LM.Lasso( alpha = 0.05,  fit_intercept = fit_intercept )
        #  print( ' Lasso CV ' )
        #  func = LM.LassoCV(  n_jobs = -1, fit_intercept = fit_intercept )
        #  print( ' Lasso LARS ' )
        #  func = LM.LassoLars( alpha = 0.05, fit_intercept = fit_intercept )

        func.fit( xTrain, yTrain )
        train_scores.append( func.score( xTrain, yTrain ) )
        test_scores.append( func.score( xTest, yTest ) )
        train_pearson.append( pearsonr( func.predict( xTrain ), yTrain ) )
                              
        test_pearson.append( pearsonr( func.predict( xTest ), yTest ) ) 
        _coefs = np.append( func.coef_, func.intercept_ )
        coefs.append( _coefs )
        mods.append( deepcopy( func ) )

    print( "The training scores: ", train_scores )
    print( "The test scores: ", test_scores )
    print( "The training pearsonR: ", train_pearson )
    print( "The test pearsonR: ", test_pearson )
        
    return( dict( coefs = coefs,
                  coefNames = xColName,
                  models = mods,
                  training_scores = train_scores,
                  test_scores = test_scores,
                  trainingR = train_pearson,
                  testR = test_pearson, 
                  xData = xData,
                  yData = yData) )

##
################################################################################################
##

def regressionModelNonFold( xName, 
                            xDesc,
                            yName,
                            yDesc,
                            timeWindows,
                            index_col = 'scheduled_timeval',
                            targCol = 'target_decay_rate_0.970',
                            clipLimits = ( 0.05, 0.05 ),
                            # startTime = dt.time( 9, 15, 0 ), 
                            # endTime = dt.time( 15, 0, 0 ),
                            winsorizeByDay = True,
                            func = LM.LassoLars( alpha = 0.05, fit_intercept = False ) ):
    '''
    Description: Linear Regression model building run through
    Flexible:    YES, please check the parameter list
    Finalized:   YES
    '''
    import glob
    import numpy as np
    from copy import deepcopy 
    from sklearn.cross_validation import LeaveOneOut as LOO
    from scipy.stats.stats import pearsonr
    xData = getSignalDataFrame( xName, xDesc, index_col )
    xColName = getSignalName( xDesc )[5:]
    yData = getSignalDataFrame( yName, yDesc, index_col )
    #  yData = sliceTime( yData, startTime, endTime )
    yData = sliceMultiTime( yData, timeWindows )
    if winsorizeByDay :
        yData = winsorizeDataByDate( yData, targCol, limits = clipLimits )
    else:
        yData = winsorizeData( yData, targCol, limits = clipLimits )
    zData = mergeDataFrame( xData, yData)
    ## zData = sliceTime( zData, startTime, endTime )
    
    ## variables to be output
    xTrain = zData[ xColName ]
    yTrain = zData[  targCol ]

    print 'Train set X has the shape :', xTrain.shape
    print 'Train set Y has the shape :', yTrain.shape

    #  func = LM.LinearRegression( fit_intercept = fit_intercept )
    #  print( ' Bayesian Ridge Regression ' )
    #  func = LM.BayesianRidge( fit_intercept = fit_intercept )
    #  print( ' Lasso Regression ' )
    #  func = LM.Lasso( alpha = 0.05,  fit_intercept = fit_intercept )
    #  print( ' Lasso CV ' )
    #  func = LM.LassoCV(  n_jobs = -1, fit_intercept = fit_intercept )
    #  print( ' Lasso LARS ' )
    #  func = LM.LassoLars( alpha = 0.05, fit_intercept = fit_intercept )

    func.fit( xTrain, yTrain )
    train_scores = func.score( xTrain, yTrain )
    train_pearson = pearsonr( func.predict( xTrain ), yTrain )
                              
    coefs = np.append( func.coef_, func.intercept_ )
    
    print( "The training scores: ", train_scores )
    print( "The training pearsonR: ", train_pearson )
        
    return( dict( coefs = coefs,
                  coefNames = xColName,
                  models = [func,],
                  training_scores = train_scores,
                  trainingR = train_pearson,
                  xData = xTrain,
                  yData = yTrain) )


##
################################################################################################
##

def linearRegressionModelOosTest( models, 
                                  xName, xDesc,
                                  yName, yDesc,
                                  timeWindows, 
                                  index_col = 'scheduled_timeval',
                                  targCol = 'target_decay_rate_0.970',
                                  isClip = False,
                                  clipLimits = ( 0.05, 0.05 ) ):  
    '''
    Description: out off sample test for models built by linearRegressionModel
    Flexible:    NO
    Finalized:   YES
    '''
    import pandas as pd
    import numpy as np
    from scipy.stats.stats import pearsonr

    xData = getSignalDataFrame( xName, xDesc, index_col )
    xColName = getSignalName( xDesc )[5:]
    yData = getSignalDataFrame( yName, yDesc, index_col )
    if isClip:
        yData = winsorizeData( yData, targCol, limits = clipLimits )
    zData = mergeDataFrame( xData, yData)
    zData = sliceMultiTime( zData, timeWindows )
    
    scores = [ models[i].score( zData[xColName], zData[targCol] ) for i in range( len( models ) ) ]
    
    yHat = pd.DataFrame( zData[ targCol ], columns = [targCol,], index = zData.index )
    for i in range( len( models ) ):
        yHat = yHat.join( pd.DataFrame( models[i].predict( zData[xColName] ), columns = [ str(i), ], index = zData.index ), how = 'outer' )
        for badI in yHat.index[[ item not in zData.index for item in yHat.index ]]:
            print badI
        for badI in zData.index[[ item not in yHat.index for item in zData.index ]]:
            print badI

    pearson = [ pearsonr( yHat[ str(i) ], yHat[ targCol ] ) for i in range( len( models ) ) ]
    
    print( 'Out of Sample Test Scores: ', scores )  
    print( 'Out of Sample Test PearsonR: ', pearson )

    fullData = zData[ xColName ].join( yHat ) 
    print( "Testing Data size: ", fullData.shape )
    
    return( dict( rsquared = scores,
                  pearsonr = pearson,
                  data = fullData ) )

##
################################################################################################

