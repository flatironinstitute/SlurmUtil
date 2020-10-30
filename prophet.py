import datetime
from collections import defaultdict

import holidays,numpy,pandas
import matplotlib.pyplot as plt
#IMPORTANT: can not import pyslurm directly or indirectly, will cuase core dump
from fbprophet             import Prophet
from fbprophet.plot        import add_changepoints_to_plot
from fbprophet.plot        import plot_cross_validation_metric
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics

IMG_DIR          = "/mnt/home/yliu/projects/slurm/utils/public/images/"

def getYearList (year=None, beforeYears=2, afterYears=1):
    if not year:
       year = datetime.date.today().year
    return list(range(year-beforeYears, year+afterYears+1))

def getHolidays(years):
    year_lst       = getYearList(beforeYears=years)
    us_ny_holidays = holidays.UnitedStates(state='NY', years=year_lst)
    us_ct_holidays = holidays.UnitedStates(state='CT', years=year_lst)
    us_nj_holidays = holidays.UnitedStates(state='NJ', years=year_lst)
    ny_ct_nj       = us_ny_holidays + us_ct_holidays + us_nj_holidays  # type is HolidaySum
    dict_holidays  = defaultdict(list)
    for date, name in ny_ct_nj.items():
        dict_holidays[name].append(numpy.datetime64(date))
    dict_l_w       = {"New Year's Day":-6,'Memorial Day':-3}
    list_df        = [pandas.DataFrame({'holiday':name, 'ds': list_date, 'lower_window':dict_l_w.get(name,0), 'upper_window':0}) for name, list_date in dict_holidays.items()]
    #list_df        = [pandas.DataFrame({'holiday':name, 'ds': list_date}) for name, list_date in dict_holidays.items()]
    df             = pandas.concat (list_df)
    return df

class MyProphet:
    def __init__(self, years=2):
        #self.holidays = self.getHolidays()
        self.holidays = getHolidays(years)
        #self.querySDBClient = SlurmDBQuery()
 
    def getHolidays(self):
        names = ['New Years Day', 'Martin Luther King, Jr. Day', 'Washington’s Birthday',
             'Good Friday',   'Memorial Day',                'Independence Day', 
             'Labor Day',     'Thanksgiving Day',            
             'Christmas']
        date  = [['2018-01-01','2019-01-01','2019-01-01','2020-01-01'], ['2018-01-15','2019-01-21'], ['2018-02-19','2019-02-18'],
             ['2018-03-30','2019-04-19'], ['2018-05-25','2018-05-28','2019-05-24','2019-05-27'], ['2018-07-04','2019-07-04'],
             ['2018-09-03','2019-09-02'], ['2017-11-23','2017-11-24','2018-11-22','2018-11-23','2019-11-28','2019-11-29'], 
             ['2017-12-25','2017-12-26','2017-12-27','2017-12-28','2017-12-29', 
              '2018-12-24','2018-12-25','2018-12-26','2018-12-27','2018-12-28',
              '2019-12-25']]

        frames = []
        for idx in range(len(names)):
            df = pandas.DataFrame({'holiday': names[idx], 'ds': pandas.to_datetime(date[idx])})
            frames.append (df)

        return pandas.concat(frames)
       
    def accountUsage_forecast (self, csv_file_template, cps=0.5, p=720):
        #start, stop, df = SlurmDBQuery.getAccountUsage_hourly()
        #df['ds']        = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
        for acct in ['cca', 'ccb', 'ccm', 'ccq']:
            # get cpu,account data
            #cpuDf = df.loc[(1,acct,),]
            #cpuAllocDf = cpuDf[['ds', 'alloc_secs']]
            #cpuAllocDf = cpuAllocDf.rename(columns={'alloc_secs': 'y'})
            csv_file       = csv_file_template.format(acct)
            cpuAllocDf     = pandas.read_csv(csv_file)
            cpuAllocDf['cap'] = 18568 * 3600
            
            model, forecast= MyProphet.analyze (cpuAllocDf, self.holidays, cps, p)
            MyProphet.sav_figure (acct, model, forecast)

    def analyze (df, holidays, cps, p):    
        # Make the prophet model and fit on the data
        #c_prophet = Prophet(changepoint_prior_scale=cps)
        #c_prophet = Prophet(growth='logistic')
        c_prophet = Prophet(growth='logistic', holidays=holidays, seasonality_mode='multiplicative')
        print("Analyze ---\n{}".format(df.head()))
        print("Holidays={}".format(holidays))
        c_prophet.fit(df)

        # Make a future dataframe
        c_forecast = c_prophet.make_future_dataframe(periods=p, freq='H')
        if 'cap' in df.columns: # always true, otherwsie, logistic model will not work
           c_forecast['cap'] = df['cap']
           lastIdx           = df.shape[0]
           lastV             = df['cap'].iloc[-1]
           c_forecast.iloc[lastIdx:,1]=lastV
        else:
           c_forecast['cap'] = 18568 * 3600

        # Make predictions
        c_forecast = c_prophet.predict(c_forecast)
        return c_prophet, c_forecast

    def sav_figure (name, c_prophet, c_forecast):
        fig = c_prophet.plot(c_forecast, xlabel = 'Date', ylabel = 'CPU Secs')
        a   = add_changepoints_to_plot(fig.gca(), c_prophet, c_forecast)
        plt.title  ('CPU Seconds (hourly)')
        plt.savefig(IMG_DIR + name + 'Forecast.png')
        #plt.show()

        figC = c_prophet.plot_components(c_forecast)
        plt.savefig(IMG_DIR + name + 'ForecastComp.png')
        #plt.show()

        # cross validation
        ca_cv = cross_validation(c_prophet, initial='200 days', horizon = '14 days', period='100 days') 
        #mean squared error (MSE), root mean squared error (RMSE), mean absolute error (MAE), mean absolute percent error (MAPE
        #ca_pm = performance_metrics(ca_cv)
        figV = plot_cross_validation_metric(ca_cv, metric='mape')
        plt.title  ('CPU Seconds Forecast MAPE (mean absolute percent error)')
        plt.savefig(IMG_DIR + name + 'ForecastCV.png')

    def clusterUsage_forecast (self, csv_file, cps=0.5, p=720):
        cpuAllocDf = pandas.read_csv(csv_file)
        model, forecast= MyProphet.analyze (cpuAllocDf, self.holidays, cps, p)
        MyProphet.sav_figure ('cluster', model, forecast)

        #cpuDownDf  = cpuDf[['ds', 'tdown_secs']]
        #cpuDownDf  = cpuDownDf.rename(columns={'tdown_secs': 'y'})
        #cpuDownDf['cap'] = 18568 * 3600
        #self.analyze (cpuDownDf, 'clusterDown', cps, p)

def main():
    inst = MyProphet(2)
    inst.clusterUsage_forecast ("./data/cpuAllocDF.csv")
    inst.accountUsage_forecast ("./data/cpuAllocDF_{}.csv")
if __name__=="__main__":
   main()
