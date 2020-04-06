## Balck Scholes Merton model can be generalized by incorporating a cost-of-carry rate b, can be 
## used to price european option on stocks,stocks with diviednd ,option on futures , currency options
# b: costo
# b = r give the black and scholes stock option model
# b = r - q  give merton stock option with continuos dividend yield q
# b = 0 give black97 future options model
# b = 0 r = 0 give Asay82 margined futures option model
# b = r-rf Garman and Kohlhagen give currency option model
from enum import Enum
import numpy as np
from scipy.stats import norm

class OptionType(Enum):
    CALL = 0
    PUT = 1


class GeneralizedBSM:

    def price(self,optionType,F,K,r,b,v,tau):
        value = 0
        if tau <= 0:
            if(optionType == OptionType.CALL):
                return max(F-K,0)
            if(optionType == OptionType.PUT):
                return max(K-F,0)
        d1 = (np.log(F/K) + (b + v * v * 0.5) * tau) / (v * np.sqrt(tau))
        d2 = d1 - v * np.sqrt(tau)
        if optionType == OptionType.CALL:
            value = \
             F *  np.exp((b-r) * tau) *norm.cdf(d1) - K * np.exp(-r * tau) * norm.cdf(d2)
        if optionType == OptionType.PUT:
            value = \
             K * np.exp(-r * tau) * norm.cdf(-d2) - F *  np.exp((b-r) * tau) *norm.cdf(-d1)
        return value
    def delta(self,optionType,F,K,r,b,v,tau):
        upF = F + F * 0.005
        downF = F - F * 0.005
        upValue = self.price(optionType,upF,K,r,b,v,tau)
        downValue = self.price(optionType,downF,K,r,b,v,tau)
        value = (upValue - downValue)  / (F * 2.0 *  0.005)
        return value
    def gamma(self,optionType,F,K,r,b,v,tau):
        upF = F + F * 0.0005
        downF = F - F * 0.0005
        midF = F
        upValue = self.price(optionType,upF,K,r,b,v,tau)
        midValue = self.price(optionType,midF,K,r,b,v,tau)
        downValue = self.price(optionType,downF,K,r,b,v,tau)
        value = (upValue - 2.0*midValue + downValue)  / (F * F * 0.0005*  0.0005)
        return value
    def vega(self,optionType,F,K,r,b,v,tau):
        upValue = self.price(optionType,F,K,r,b,v+0.01,tau)
        downValue = self.price(optionType,F,K,r,b,v,tau)
        value = (upValue - downValue)
        return value
    def rho(self,optionType,F,K,r,b,v,tau):
        upValue = self.price(optionType,F,K,r+0.0001,b,v,tau)
        downValue = self.price(optionType,F,K,r,b,v,tau)
        value = (upValue - downValue)
        return value
    def theta(self,optionType,F,K,r,b,v,tau,dt):
        upValue = self.price(optionType,F,K,r,b,v,tau - dt)
        downValue = self.price(optionType,F,K,r,b,v,tau)
        value = (upValue - downValue) / dt
        return value
class SpreadModel(object):
    def price(self,optionType,f1,f2,v1,v2,
    q1,q2,b1,b2,corr,K,r,tau):
        S = (q1 * f1 * np.exp((b1 - r) * tau)) / (q2 * f2 * np.exp((b2 - r) * tau) + K * np.exp(-r * tau))
        F = (q2 * f2 * np.exp((b2 - r) * tau)) / (q2 * f2 * np.exp((b2 - r) * tau) + K * np.exp(-r * tau))
        v = np.sqrt(v1**2 + (v2*F)**2 - 2 * corr * v1 * v2 * F)
        d1 = (np.log(S) + (v*v*0.5)*tau) / (v * np.sqrt(tau))
        d2 = d1 - v * np.sqrt(tau)


        value = 0
        if tau <= 0:
            if(optionType == OptionType.CALL):
                return max(f1 * q1 - f2 * q2 - K,0)
            if(optionType == OptionType.PUT):
                return max(K - f1 * q1 + f2 * q2,0)
        if optionType == OptionType.CALL:
            value = \
             (q2 * f2 * np.exp((b2 - r) * tau) + K * np.exp(-r * tau)) * (S * norm.cdf(d1) - norm.cdf(d2))
             
        if optionType == OptionType.PUT:
            value = \
             (q2 * f2 * np.exp((b2 - r) * tau) + K * np.exp(-r * tau)) * (norm.cdf(-d2) - S * norm.cdf(-d1))
        return value
if __name__ == '__main__':
    spmodel = SpreadModel()
    # corr 44%  oi:11.80% rm:17.54% oi-2*rm  oi:6479 rm:2139 K:2201
    #print spmodel.price(OptionType.CALL,6479,2139,0.118,0.1754,1,2,0.05,0.05,0.55,2201,0.05,21.0/244.0) / 2201.0
    #print spmodel.price(OptionType.CALL,6479,2139,0.118,0.1754,1,2,0.05,0.05,0.44,2201,0.05,21.0/244.0) / 2201.0
    #print spmodel.price(OptionType.CALL,6479,2139,0.118,0.1754,1,2,0.05,0.05,0.37,2201,0.05,21.0/244.0) / 2201.0
    # corr 54.89 y:5568 m :2628 y - 2m, y:12.71% m:16.29% T:49 k 312
    # m_v_dict = {'m_bid':0.2793,'m_ask':0.3555,'m_mid':0.3174} # 1905
    # y_v_dict = {'y_bid':0.2695,'y_ask':0.3455,'y_mid':0.3174} # 1909
    # m_y_corr_dict = {'corr_bid':0.99,'corr_ask':0.90}
    #
    # for m_k,m_v in m_v_dict.iteritems():
    #     for y_k,y_v in y_v_dict.iteritems():
    #         for corr_k,corr_v in m_y_corr_dict.iteritems():
    #             print m_k,m_v,y_k,y_v,corr_k,corr_v
    #             print spmodel.price(OptionType.CALL,579.0,624.0,y_v,m_v,1,1,0.05,0.05,corr_v,-45,0.05,21.0/244.0)


    print "protect strategy rb1901 - rb2005 = 139 ATM 4M"
    m_v_dict = {'m_bid':0.18,'m_ask':0.27,'m_mid':0.24} # 1905
    y_v_dict = {'y_bid':0.18,'y_ask':0.27,'y_mid':0.24} # 1909
    m_y_corr_dict = {'corr_bid':0.99,'corr_ask':0.90}

    for m_k,m_v in m_v_dict.iteritems():
        for y_k,y_v in y_v_dict.iteritems():
            for corr_k,corr_v in m_y_corr_dict.iteritems():
                print m_k,m_v,y_k,y_v,corr_k,corr_v
                print spmodel.price(OptionType.PUT,3577.0,3438.0,y_v,m_v,1,1,0.05,0.05,corr_v,139,0.05,84.0/244.0)

    print "compensate strategy rb1901 - rb2005 = 139 OTM K 300 4M"
    m_v_dict = {'m_bid':0.18,'m_ask':0.27,'m_mid':0.24} # 1905
    y_v_dict = {'y_bid':0.18,'y_ask':0.27,'y_mid':0.24} # 1909
    m_y_corr_dict = {'corr_bid':0.99,'corr_ask':0.90}

    for m_k,m_v in m_v_dict.iteritems():
        for y_k,y_v in y_v_dict.iteritems():
            for corr_k,corr_v in m_y_corr_dict.iteritems():
                print m_k,m_v,y_k,y_v,corr_k,corr_v
                print spmodel.price(OptionType.CALL,3577.0,3438.0,y_v,m_v,1,1,0.05,0.05,corr_v,300,0.05,84.0/244.0)