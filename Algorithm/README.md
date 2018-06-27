## Design and Flow
<img src="https://swkxrg.dm.files.1drv.com/y4mRgJuaoqWLkQ90OzjkIjVE8_HgnCcqmo2nx9jb_9mEJuou8t9ZORrU6ZRHT1G7LHQQNeb7qcMdud5BYL1NILT4Eqd2_0VpooyS8gDQ0du4UV1c3XBnHY3j350TL8ybf45MgAZDTzXHyj38M0qgICIdgYn5O9nA7DKEoACjC5X-uOTN4tFN7QyH9SYt3GVaRtN3SgJQ4DeC_AYtIM7JakrPQ?width=1024&height=576&cropmode=none" width="1024" height="576" />

---

## Conceptual Framework

The algorithm runs every minute and uses 'Logic.py' to determine whether to Buy, Sell, or Hold.

The algorithm is designed to look for meaningful deviations from the average price. It does this by using three technical indicators; MACD Signal, MACD Cross, and a simple moving average. For each indicator, a moving average and standard deviation is calculated and used to identify "meaningful deviations" from the average.

If 'Buy' or 'Sell' signal has been identified, an order size is calculated using your account balance, Relative Strength Index (RSI), Williams % R (WMS), and a weighted-average of regression line slopes. Refer to [technical_indicators.py](https://github.com/Jacyle/binance-technical-algorithm/blob/master/Algorithm/technical_indicators.py) for the formulas. Once an order size is calculated and it is of sufficient size as to adhere to the Binance minimum/maximum order size rules, the order is placed, otherwise it is rejected by the algorithm and is recorded in the trade_log. It's designed this way to record and monitor the behavior of the algorithm, for example, when it decides to Buy/Sell/Hold and the order sizes being calculated at a given time.  

---
