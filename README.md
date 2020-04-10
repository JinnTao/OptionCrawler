# OptionCrawler
---
_行业期权数据采集（主要从微信小程序\网站上采集数据，寻找套利机会）_
## 简介
> 在以往实际的场外期权交易中，为了最优化报价以及控制风险，需要了解同行业竞争对手，从而开发出当前工具。
> 在对同业报价进行审核之后，将其昨日期权价格转化为波动率数据，以此为依据来对我们预测的波动率进行修正。
> 目前采集的公司有：

* 渤海期货
* 鲁证期货
* 新湖期货
* 华泰期货
* 南华期货
* 济海贸发
* 上海中期
* 浙商期货
* 等等
## 流程说明
&emsp;简单来说就是将市面上的期货公司的期权报价数据收集起来，转化为波动率的过程。实现方法有很多种，目前我主要用的包是 python requests/grequests。首先我们找到一些期货公司，这里拿新湖瑞风 举个例子：https://xinhu.tongyuquant.com:9140/ ，分析网页可以发现，表头构造如下：
```

POST /bct/quote HTTP/1.1
Host: xinhu.tongyuquant.com
Connection: keep-alive
Content-Length: 164
Accept: application/json, text/plain, */*
Sec-Fetch-Dest: empty
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36
Content-Type: application/x-www-form-urlencoded
Origin: https://xinhu.tongyuquant.com:9140
Sec-Fetch-Site: same-site
Sec-Fetch-Mode: cors
Referer: https://xinhu.tongyuquant.com:9140/option?code=AP010
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9

```


```
   header = {
                'Origin': 'https://xinhu.tongyuquant.com:9140',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/70.0.3538.77 Chrome/70.0.3538.77 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json, text/plain, */*',
                'Referer': 'https://xinhu.tongyuquant.com:9140/option?code=%s' % symbol,
                'Connection': 'keep-alive'
            }
```


'''

&emsp;上面品种是 AP010（对应symbol值等于AP010）, 注意这里到期日有很多，我们一般获取10、15、20、21、30、42交易日（不存在时，用线性插值计算）,然后转化为对应波动率（在BSM公式中使用梯度下降）保存到数据库中。这时候我们在数据库中会有如下表格：

Date|ComId|InstID|Vol1_10|Vol_15
---|:---|:--:|---:|---:
2020-04-10|新湖|AP010|10%|15%
2020-04-10|其他|AP010|10%|15%


&emsp;有了上面的数据再加上我们自己波动率预测值，就可以很好的报出当天价格了。



__以后有时间再陆续增加.__


