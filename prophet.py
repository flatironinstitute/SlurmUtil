#!/usr/bin/env python
import argparse
import datetime, glob, os, time
import holidays,numpy,pandas
import matplotlib.pyplot as plt

from collections import defaultdict
#IMPORTANT: can not import pyslurm directly or indirectly, will cuase core dump
from fbprophet             import Prophet
from fbprophet.plot        import add_changepoints_to_plot
from fbprophet.plot        import plot_cross_validation_metric
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics

IMG_DIR          = "./public/images/"

def getTS (t, formatStr='%Y-%m-%d %H:%M:%S'):
    return int(time.mktime(time.strptime(t, formatStr)))

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

def getDFDays (df):
    t1         = df.iat[0,0]
    t2         = df.iat[len(df)-1,0]
    data_days  = int((getTS(t2)-getTS(t1))/3600/24)
    return data_days

def getFileNameNoExt(fname):
    return os.path.splitext(os.path.split(fname)[1])[0]

class MyProphet:
    def __init__(self, years=2):
        self.holidays = getHolidays(years)
 
    #TODO: cps not used
    def clusterUsage_forecast (self, csv_file, cps=0.5, days=14):
        cpuAllocDf = pandas.read_csv(csv_file)
        data_days  = getDFDays(cpuAllocDf)
        
        model, forecast = MyProphet.predict (cpuAllocDf, self.holidays, cps, days, data_days=days)
        cv              = MyProphet.validation (model, data_days) 
        name            = getFileNameNoExt (csv_file)
        MyProphet.sav_figure (name, model, forecast, cv)

        #cpuDownDf  = cpuDf[['ds', 'tdown_secs']]
        #cpuDownDf  = cpuDownDf.rename(columns={'tdown_secs': 'y'})
        #cpuDownDf['cap'] = 18568 * 3600
        #self.predict (cpuDownDf, 'clusterDown', cps, p)
        return days

    def accountUsage_forecast (self, csv_file_regex, cps=0.5, days=14):
        f_lst  = glob.glob(csv_file_regex)
        for csv_file in f_lst:
            # get cpu,account data
            cpuAllocDf     = pandas.read_csv(csv_file)
            data_days      = getDFDays(cpuAllocDf)
            #cpuAllocDf['cap'] = 18568 * 3600
            
            model, forecast = MyProphet.predict (cpuAllocDf, self.holidays, cps, days, data_days)
            cv              = MyProphet.validation (model, data_days) 
            name            = getFileNameNoExt (csv_file)
            MyProphet.sav_figure (name, model, forecast, cv)

    def predict (df, holidays, cps, future_days, data_days=365):    
        # Make the prophet model and fit on the data
        #c_prophet = Prophet(changepoint_prior_scale=cps)
        c_prophet = Prophet(growth='logistic', holidays=holidays, seasonality_mode='multiplicative')
        print("Analyze ---\n{}".format(df.head()))
        print("Holidays={}".format(holidays))
        c_prophet.fit(df)
        # Make a future dataframe
        c_forecast = c_prophet.make_future_dataframe(periods=future_days*24, freq='H')
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

    def validation (c_prophet, data_days, horizon_days=14):
        initial_days = min (200, int(data_days/2))
        print("validation data_days={},initial={},horizon_days={}".format(data_days,initial_days,horizon_days))
        
        # cross validation, 
        # By default, the initial training period is set to three times the horizon, and cutoffs (period) are made every half a horizon.
        #ca_cv = cross_validation(c_prophet, initial='200 days', horizon = '14 days', period='100 days') 
        ca_cv = cross_validation(c_prophet, initial='{} days'.format(initial_days), horizon = '{} days'.format(horizon_days)) 
        #mean squared error (MSE), root mean squared error (RMSE), mean absolute error (MAE), mean absolute percent error (MAPE
        #ca_pm = performance_metrics(ca_cv)
        return ca_cv

    def sav_figure (name, c_prophet, c_forecast, ca_cv):
        fig = c_prophet.plot(c_forecast, xlabel = 'Date', ylabel = 'CPU Secs')
        a   = add_changepoints_to_plot(fig.gca(), c_prophet, c_forecast)
        plt.title  ('CPU Seconds (hourly)')
        plt.savefig(IMG_DIR + name + '_forecast.png')
        #plt.show()

        figC = c_prophet.plot_components(c_forecast)
        plt.savefig(IMG_DIR + name + '_forecastComp.png')
        #plt.show()

        figV = plot_cross_validation_metric(ca_cv, metric='mape')
        plt.title  ('CPU Seconds Forecast MAPE (mean absolute percent error)')
        plt.savefig(IMG_DIR + name + '_forecastCV.png')

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Make prediction of cluster and account usage.')
    parser.add_argument('-y', '--years',     type=int, default=2,   help='years of history used to make prediction')
    parser.add_argument('-d', '--future',    type=int, default=14,  help='days in the future to predict')
    #parser.add_argument('-c', '--clusters', nargs="+", default=['cluster'],   help='clusters')
    #parser.add_argument('-u', '--update', default=False, action='store_true', help='training data updated before making prediction')
    args = parser.parse_args()
    #print(args)

    inst = MyProphet(args.years)
    inst.clusterUsage_forecast ("./data/slurm_cpuAllocDF.csv",   days=args.future)
    inst.accountUsage_forecast ("./data/slurm_cpuAllocDF_*.csv", days=args.future)
