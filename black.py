#encoding:utf-8

from enum import Enum
from scipy.stats import norm
import numpy as np
import QuantLib as ql
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
QL_EPSILON = 1e-6
QL_MAX_REAL = 1e10
QL_MIN_REAL = -1e10
M_SQRT_2   = 0.707106781186547524400
M_1_SQRTPI = 0.564189583547756286948

def close(a, b):
    return abs(a-b)<=QL_EPSILON

class OptionType(Enum):
    CALL = 0
    PUT = 1

class BlackCalc:
    
    def __init__(self, optionType, strike, forward, stdDev, discount):
        self.strike_ = strike
        self.forward_ = forward
        self.stdDev_ = stdDev
        self.discount_ = discount
        self.variance_ = stdDev*stdDev

        self.initialize(optionType)

    def initialize(self, optionType):
        assert self.strike_>=0.0, "strike {} must be non-negative".format(self.strike_)
        assert self.forward_>0.0, "forward {} must be positive".format(self.forward_)
        assert self.stdDev_>=0.0, "stdDev {} must be non-negative".format(self.stdDev_)
        assert self.discount_>0.0, "discount {} must be positive".format(self.discount_)
        self.d1_ = QL_MAX_REAL
        self.d2_ = QL_MAX_REAL
        self.cum_d1_ = 1.0
        self.cum_d2_ = 1.0
        self.n_d1_ = 0.0
        self.n_d2_ = 0.0
        if self.stdDev_>=QL_EPSILON:
            if close(self.strike_, 0.0):
                d1_ = QL_MAX_REAL
                d2_ = QL_MAX_REAL
                cum_d1_ = 1.0
                cum_d2_ = 1.0
                n_d1_ = 0.0
                n_d2_ = 0.0
            else:
                d1_ = np.log(self.forward_/self.strike_)/self.stdDev_ + 0.5*self.stdDev_
                d2_ = d1_-self.stdDev_
                cum_d1_ = norm.cdf(d1_)
                cum_d2_ = norm.cdf(d2_)
                n_d1_ = norm.pdf(d1_)
                n_d2_ = norm.pdf(d2_)
        else:
            if close(self.forward_, self.strike_):
                d1_ = 0
                d2_ = 0
                cum_d1_ = 0.5
                cum_d2_ = 0.5
                n_d1_ = M_SQRT_2 * M_1_SQRTPI
                n_d2_ = M_SQRT_2 * M_1_SQRTPI
            elif self.forward_>self.strike_:
                d1_ = QL_MAX_REAL
                d2_ = QL_MAX_REAL
                cum_d1_ = 1.0
                cum_d2_ = 1.0
                n_d1_ = 0.0
                n_d2_ = 0.0
            else:
                d1_ = QL_MIN_REAL
                d2_ = QL_MIN_REAL
                cum_d1_ = 0.0
                cum_d2_ = 0.0
                n_d1_ = 0.0
                n_d2_ = 0.0

        self.d1_ = d1_
        self.d2_ = d2_
        self.x_ = self.strike_
        self.DxDstrike_ = 1.0

        # the following one will probably disappear as soon as
        # super-share will be properly handled
        self.DxDs_ = 0.0

        # this part is always executed.
        # in case of plain-vanilla payoffs, it is also the only part
        # which is executed.
        if optionType==OptionType.CALL:
            self.alpha_     =  cum_d1_#  N(d1)
            self.DalphaDd1_ =    n_d1_#  n(d1)
            self.beta_      = -cum_d2_# -N(d2)
            self.DbetaDd2_  = -  n_d2_# -n(d2)
        elif optionType==OptionType.PUT:
            self.alpha_     = -1.0+cum_d1_# -N(-d1)
            self.DalphaDd1_ =        n_d1_#  n( d1)
            self.beta_      =  1.0-cum_d2_#  N(-d2)
            self.DbetaDd2_  =     -  n_d2_# -n( d2)
        else:
            raise Exception("invalid option type {}".format(optionType))


    def value(self):
        v = self.discount_ * (self.forward_ * self.alpha_ + self.x_ * self.beta_)
        return v

    def delta(self):
        temp = self.stdDev_*self.forward_
        DalphaDforward = self.DalphaDd1_/temp
        DbetaDforward  = self.DbetaDd2_/temp
        temp2 = DalphaDforward * self.forward_ + self.alpha_ \
              + DbetaDforward  * self.x_; # DXDforward = 0.0

        return self.discount_ * temp2

    def gamma(self):
        temp = self.stdDev_*self.forward_
        DalphaDforward = self.DalphaDd1_/temp
        DbetaDforward  = self.DbetaDd2_/temp

        D2alphaDforward2 = - DalphaDforward / self.forward_*(1+self.d1_/self.stdDev_)
        D2betaDforward2  = - DbetaDforward / self.forward_*(1+self.d2_/self.stdDev_)

        temp2 = D2alphaDforward2 * self.forward_ + 2.0 * DalphaDforward \
            +D2betaDforward2  * self.x_

        return self.discount_ * temp2

    def vanna(self, maturity):
        assert maturity>=0.0, "negative maturity not allowed"

        temp = self.stdDev_*self.forward_
        DalphaDforward = self.DalphaDd1_/temp
        DbetaDforward  = self.DbetaDd2_/temp

        temp2 = np.log(self.strike_/self.forward_)/self.variance_
        DalphaDsigma = self.DalphaDd1_*(temp2+0.5)*np.sqrt(maturity)

        D2alphaDforwardDsigma = -DalphaDforward*self.d1_*(temp2+0.5) - self.DalphaDd1_/self.stdDev_*np.sqrt(maturity)
        D2betaDforwardDsigma  = -DbetaDforward*self.d2_*(temp2-0.5) - self.DbetaDd2_/self.stdDev_*np.sqrt(maturity)

        temp3 = D2alphaDforwardDsigma * self.forward_ + DalphaDsigma \
              + D2betaDforwardDsigma  * self.x_ # DXDforward = 0.0

        return self.discount_ * temp3

    def thetaPerDay(self,rate,vol):
        return -rate * self.value() / 365.0 - 0.5 * vol * vol \
        * self.forward_ * self.forward_ * self.gamma() / 252

    def vega(self,maturity):
        assert maturity>=0.0, "maturity {} must be not negative".format(maturity)
        temp = np.log(self.strike_/self.forward_)/self.variance_
        DalphaDsigma = self.DalphaDd1_*(temp+0.5)*np.sqrt(maturity)
        DbetaDsigma  = self.DbetaDd2_ *(temp-0.5)*np.sqrt(maturity)

        temp2 = DalphaDsigma * self.forward_ + DbetaDsigma * self.x_

        return self.discount_ * temp2


class PriceError:
    def __init__(self,optionType,strike,forward,price,maturity,discount):
        self.optionType = optionType
        self.strike = strike
        self.forward = forward
        self.price = price
        self.maturity = maturity
        self.discount = discount
    def __call__(self,vol):
        #print self.optionType,self.strike,self.forward,vol*np.sqrt(maturity),self.discount,vol,maturity
        bc = BlackCalc(self.optionType,self.strike,self.forward,vol*np.sqrt(self.maturity),self.discount)
        return bc.value() - self.price

class BlackImpliedCalculator:
    def __init__(self,optionType,strike,forward,maturity,discount = 1.0):
        self.optionType = optionType
        self.strike = strike
        self.forward = forward
        self.maturity = maturity
        self.discount = discount
        self.solver = ql.Brent()
    def obtainImpliedVol(self,price,accuracy = 1.0e-4,maxEvaluations = 100,minVol = 1.0e-7, maxVol = 1000.0):
        try:
            p = PriceError(self.optionType,self.strike,self.forward,price,self.maturity,self.discount)
            self.solver.setMaxEvaluations(maxEvaluations)
            guess = (minVol + maxVol) / 2.0
            vol = self.solver.solve(p,accuracy,guess,minVol,maxVol)
            return vol
        except:
            print 'optPrc:',price,'X:',self.strike,'F',self.forward,'T',self.maturity,self.discount,'Error'
            return np.nan

if __name__ == "__main__":
    '''test black example'''

    model_option_type = OptionType.CALL
    maturity =  0.0356164383562
    # # print model.value(),model.delta(),model.gamma(),\
    # # model.thetaPerDay(rate,vol) ,model.vega(maturity)   

    # # print 'value',model.value(),'vega',model.vega(maturity),'vegaCash:',model.vega(maturity) * 0.28
    # # print model.thetaPerDay(rate,vol) * 21.0
    model_impiledVol = BlackImpliedCalculator(model_option_type,4984.0,4984.0, maturity,0.9989320774848635)
    imp_vol = model_impiledVol.obtainImpliedVol(45.0)
    print imp_vol
    # # print imp_vol
  
    vol_list = np.arange(0.1,0.5,0.02)
    iv_list = []

    for vol in vol_list:


        stdDev = vol * np.sqrt(maturity)
        rate = 0.03
        discount = np.exp(-rate*maturity)
        model = BlackCalc(model_option_type,1,1,stdDev,discount)
        # print model.value(),model.delta(),model.gamma(),\
        # model.thetaPerDay(rate,vol) ,model.vega(maturity)    sell_imp_vol init_valuation_vol

        # print 'value',model.value(),'vega',model.vega(maturity),'vegaCash:',model.vega(maturity) * 0.28
        # print model.thetaPerDay(rate,vol) * 21.0
        model_impiledVol = BlackImpliedCalculator(model_option_type,1.0,1.0,maturity,discount)
        print model.value()
        imp_vol = model_impiledVol.obtainImpliedVol(model.value())
        iv_list.append(imp_vol)
        print 'rv:',vol,'value:',model.value(),'imp_vol:',imp_vol,'diff',vol- imp_vol
    plt.plot(np.array(range(len(iv_list))),np.array(iv_list),label='iv')
    plt.plot(np.array(range(len(vol_list))),np.array(vol_list),label='vol')
    plt.plot(np.array(range(len(vol_list))),np.array(vol_list - iv_list),label='diff')
    plt.xlabel('hv label')
    plt.ylabel('iv label')

    plt.title("Differ")

    plt.legend()

    plt.show()




