import pandas as pd
from pandas.core.frame import DataFrame
from database import Database_Connector 
from functools import reduce
import numpy as np
import matplotlib.pyplot as plt

# caluclation of earnigs yield = earnings per share
eps_data = pd.read_csv('data/earnings_latest.csv')
eps_data_clean = eps_data[eps_data['eps'].notnull()]
eps_data_with_year = eps_data_clean.assign(year=lambda row: row['date'].apply(lambda x: x[0:4]))
eps_data_with_year = eps_data_with_year.rename(columns={'symbol':'Ticker'})
eps_data_2019_inter = eps_data_with_year.query('year=="2019"')
eps_data_2019_inter = eps_data_2019_inter.groupby('Ticker').aggregate({'eps': 'mean'})['eps']
eps_data_2019_inter = eps_data_2019_inter.multiply(4)

# roic caluclation helper
capital_data = pd.read_csv('data/us-balance-annual.csv',sep=';')
profit_data = pd.read_csv('data/us-income-annual.csv',sep = ';')

roic_annual_data = DataFrame()
roic_annual_data['Ticker'] = capital_data['Ticker']  
roic_annual_data['Fiscal Year'] = capital_data['Fiscal Year']
roic_annual_data['Investments'] = capital_data['Total Assets'] - capital_data['Total Current Liabilities']
roic_annual_data = roic_annual_data.assign(Net_Income= lambda row: profit_data.loc[(profit_data['Ticker'] == row['Ticker']) 
                                            & (profit_data['Fiscal Year'] == row['Fiscal Year'])]
                                        ["Net Income"])
roic_annual_data['roic'] = roic_annual_data['Net_Income'] /roic_annual_data['Investments']

# book value Caluclation
def calucalte_book_value(ticker,year):
    shares = profit_data.loc[(profit_data['Ticker'] == ticker) & (profit_data['Fiscal Year'] == year)]['Shares (Basic)']
    investments = roic_annual_data.loc[(roic_annual_data['Ticker'] == ticker) & (roic_annual_data['Fiscal Year'] == year)]['Investments']
    return shares/investments

# roic into years and avg
roic_annual_data = roic_annual_data.assign(bvps = lambda row: calucalte_book_value(row['Ticker'],row['Fiscal Year'])) 
roic_5_avg_inter = roic_annual_data.groupby('Ticker').aggregate({'roic':'mean'})['roic']

# 1.eps 2020 = eps_data_2020
eps_data_2019 = DataFrame()
eps_data_2019['Ticker'] = eps_data_2019_inter.keys()
eps_data_2019['eps'] = eps_data_2019_inter.values

# 2.roic 2020 = roic_2020
roic_2019 = DataFrame()
roic_2019[['Ticker','roic']] = roic_annual_data.loc[roic_annual_data['Fiscal Year']== 2019][['Ticker','roic']]

# 3. Price to Book = book_price_2020
book_price_2019 = DataFrame()
book_price_2019[['Ticker','bvps']] = roic_annual_data.loc[roic_annual_data['Fiscal Year']== 2019][['Ticker','bvps']]

# 4. previous 5 years roic avg = roic_5_avg
roic_5_avg = DataFrame()
roic_5_avg['Ticker'] = roic_5_avg_inter.keys()
roic_5_avg['roic_avg'] = roic_5_avg_inter.values

# final data normalized and took all the data and took common components
final_data = DataFrame()
final_data['Ticker'] = roic_2019['Ticker']
final_data = pd.merge(roic_2019,eps_data_2019,on='Ticker',how="inner")
final_data = pd.merge(final_data,book_price_2019,on='Ticker',how="inner")
final_data = pd.merge(final_data,roic_5_avg,on='Ticker',how="inner")

# now lets assign ranks individually
final_data['eps_rank'] = final_data['eps'].rank(method='min',ascending= False)
final_data['roic_rank'] = final_data['roic'].rank(method='min',ascending= False)
final_data['bvps_rank'] = final_data['bvps'].rank(method='min',ascending= False)
final_data['roic_avg_rank'] = final_data['roic_avg'].rank(method='min',ascending=False)
final_data = final_data.assign(rank_total = lambda row : row['eps_rank']
                                                        + row['roic_rank']
                                                        + row['bvps_rank']
                                                        + row['roic_avg_rank'])

# data is done 
final_data = final_data.sort_values(by='rank_total')
companies_list = tuple(final_data.head(11)['Ticker'])

# 1. 5 years - 2015 - 2015-01-02
# 1. 4 years - 2016 - 2016-01-04
# 1. 3 years - 2017 - 2017-01-03
# 1. 2 years - 2018 - 2018-01-02
# 1. 1 years - 2019 - 2019-01-02
# 2. present - 2020 - 2020-01-02

# Stock data_set columns 
stock_data_columns = ['Ticker','date','open','high','low','close','close_adjusted','volume','split_coefficient']
stock_2020 = DataFrame(Database_Connector().get_ticker_data(companies_list,'2020-01-02'),columns=stock_data_columns)[['Ticker','close_adjusted']]
stock_2019 = DataFrame(Database_Connector().get_ticker_data(companies_list,'2019-01-02'),columns=stock_data_columns)[['Ticker','close_adjusted']]
stock_2018 = DataFrame(Database_Connector().get_ticker_data(companies_list,'2018-01-02'),columns=stock_data_columns)[['Ticker','close_adjusted']]
stock_2017 = DataFrame(Database_Connector().get_ticker_data(companies_list,'2017-01-03'),columns=stock_data_columns)[['Ticker','close_adjusted']]
stock_2016 = DataFrame(Database_Connector().get_ticker_data(companies_list,'2016-01-04'),columns=stock_data_columns)[['Ticker','close_adjusted']]
stock_2015 = DataFrame(Database_Connector().get_ticker_data(companies_list,'2015-01-02'),columns=stock_data_columns)[['Ticker','close_adjusted']]

# normalizing all the sizes
ror_2020_temp = pd.merge(stock_2020,stock_2015,on='Ticker' , how='inner')
ror_2019_temp = pd.merge(stock_2019,stock_2015,on='Ticker' , how='inner')
ror_2018_temp = pd.merge(stock_2018,stock_2015,on='Ticker' , how='inner')
ror_2017_temp = pd.merge(stock_2017,stock_2015,on='Ticker' , how='inner')
ror_2016_temp = pd.merge(stock_2016,stock_2015,on='Ticker' , how='inner')

# finding actual ror
ror_2020 = ror_2020_temp.assign(ror_2020 = lambda row : row['close_adjusted_x']/row['close_adjusted_y'])
ror_2019 = ror_2019_temp.assign(ror_2019= lambda row : row['close_adjusted_x']/row['close_adjusted_y'])
ror_2018 = ror_2018_temp.assign(ror_2018 = lambda row : row['close_adjusted_x']/row['close_adjusted_y'])
ror_2017 = ror_2017_temp.assign(ror_2017 = lambda row : row['close_adjusted_x']/row['close_adjusted_y'])
ror_2016 = ror_2016_temp.assign(ror_2016 = lambda row : row['close_adjusted_x']/row['close_adjusted_y'])

# dropping unessary values
ror_2020 = ror_2020.drop(columns=['close_adjusted_x','close_adjusted_y'])
ror_2019 = ror_2019.drop(columns=['close_adjusted_x','close_adjusted_y'])
ror_2018 = ror_2018.drop(columns=['close_adjusted_x','close_adjusted_y'])
ror_2017 = ror_2017.drop(columns=['close_adjusted_x','close_adjusted_y'])
ror_2016 = ror_2016.drop(columns=['close_adjusted_x','close_adjusted_y'])


data_frames = [ror_2020,ror_2019,ror_2018,ror_2017,ror_2016]

ror_data = reduce(lambda left,right: pd.merge(left,right,on=['Ticker'],how='inner'),data_frames)


# rounding of ror for every Data
ror_data = ror_data.astype({'ror_2020':'float',
                            'ror_2019':'float',
                            'ror_2018':'float',
                            'ror_2017':'float',
                            'ror_2016':'float'}).round(decimals=5)

# TODO:
# 4. Make a video of it

years = ['2016','2017','2018','2019','2020']

fig, ax = plt.subplots(1)
for ticker in ror_data['Ticker'].tolist():
    data = ror_data.loc[ror_data['Ticker'] == ticker].drop(columns=['Ticker']).values.tolist()[0]
    plt.plot(years,data,"-o",label=ticker)
plt.legend()
plt.show()


average_ror = ror_data.mean(numeric_only=True)
# year wise average ror of s and p 
# 2016	11.96%
# 2017	21.83%
# 2018	-4.38%
# 2019	31.49%
# 2020	18.40%
s_and_p_cumulative_avg= [1.1196,1.36,1.3042,1.71,2.02]

plt.plot(years,s_and_p_cumulative_avg,"-o",label='S&P500')
plt.plot(years,average_ror,"-o",label='ERP5')
plt.legend()
plt.show()


def main():
    print(ror_data)

if __name__=="__main__":
    main()
