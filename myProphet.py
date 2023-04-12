#!/usr/bin/env python
import argparse
import datetime, glob, os, time
import holidays,numpy,pandas
import matplotlib.pyplot as plt

from collections import defaultdict
#IMPORTANT: can not import pyslurm directly or indirectly, will cuase core dump
from prophet             import Prophet
from prophet.plot        import add_changepoints_to_plot
from prophet.plot        import plot_cross_validation_metric
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics

IMG_DIR          = "./public/images/"
CSV_DIR          = "./data/"

#maintaince time, 'eventname':[start,end,real_ned]'
maintenance = {
               'Maint201812': ['2018-12-14 17:00:00','2018-12-18 09:00:00',None], #guess finish time
               'Maint201903': ['2019-03-22 17:00:00','2019-03-25 09:00:00',None], #guess finish time
               'Maint201908': ['2019-08-23 17:00:00','2019-08-26 09:00:00',None],
               'Slurm20.02':  ['2020-11-06 17:00:00','2020-11-09 08:00:00','2020-11-08 11:24:00']}

def getTS (t, formatStr='%Y-%m-%d %H:%M:%S'):
    return int(time.mktime(time.strptime(t, formatStr)))

def getYearList (year=None, beforeYears=2, afterYears=1):
    #Prior scales can be set separately for individual holidays by including a column prior_scale in the holidays dataframe. 
    if not year:
       year = datetime.date.today().year
    return list(range(year-beforeYears, year+afterYears+1))

def getHolidays(years):
    year_lst       = getYearList(beforeYears=years, afterYears=1)
    us_ny_holidays = holidays.UnitedStates(state='NY', years=year_lst)
    us_ct_holidays = holidays.UnitedStates(state='CT', years=year_lst)
    us_nj_holidays = holidays.UnitedStates(state='NJ', years=year_lst)
    ny_ct_nj       = us_ny_holidays + us_ct_holidays + us_nj_holidays  # type is HolidaySum
    dict_holidays  = defaultdict(list)
    for date, name in ny_ct_nj.items():
        dict_holidays[name].append(numpy.datetime64(date))
    #It is often important to include effects for a window of days around a particular holiday
    dict_l_w       = {"New Year's Day":-6,'Memorial Day':-3}
    list_df        = [pandas.DataFrame({'holiday':name, 'ds': list_date, 'lower_window':dict_l_w.get(name,0), 'upper_window':0}) for name, list_date in dict_holidays.items()]
    df             = pandas.concat (list_df)
    return df

#how many days are covered by df
def getDFDays (df):
    t1         = df.iat[0,0]
    t2         = df.iat[len(df)-1,0]
    data_days  = int((getTS(t2)-getTS(t1))/3600/24)
    return data_days

def getFileNameNoExt(fname):
    return os.path.splitext(os.path.split(fname)[1])[0]

def notInMaint (row):
    for m, md in maintenance.items():
        st, et = md[0], md[1]
        if (row['ds'] > st) and (row['ds'] < et):
           return False
    return True

class MyProphet:
    def __init__(self, years=3):
        self.holidays = getHolidays(years)
 
    def prepareData(csv_file):
        df              = pandas.read_csv(csv_file)
        df              = df[df['y']>0]
        b_col           = df.apply(notInMaint, axis=1)
        cpuAllocDf      = df[b_col]
        #cpuAllocDf      = pandas.read_csv(csv_file)
        data_days       = getDFDays(cpuAllocDf)
        return cpuAllocDf,data_days

    #TODO: cps not used
    def clusterUsage_forecast (self, csv_file, cps=0.5, days=14):
        #cpuAllocDf      = pandas.read_csv(csv_file)
        #data_days       = getDFDays(cpuAllocDf)
        cpuAllocDf,data_days = MyProphet.prepareData(csv_file)
        
        model, forecast = MyProphet.predict (cpuAllocDf, self.holidays, cps, days)
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
            
            model, forecast = MyProphet.predict (cpuAllocDf, self.holidays, cps, days)
            cv              = MyProphet.validation (model, data_days) 
            name            = getFileNameNoExt (csv_file)
            MyProphet.sav_figure (name, model, forecast, cv)

    def predict (df, holidays, changepoint_prior_scale, future_days):    
        # Make the prophet model and fit on the data
        #c_prophet = Prophet(changepoint_prior_scale=cps)
        #Prophet allows you to make forecasts using a logistic growth trend model, with a specified carrying capacity. 
        #manually specify the locations of potential changepoints with the changepoints argument. Slope changes will then be allowed only at these points,m = Prophet(changepoints=['2014-01-01'])
        #the seasonality is not a constant additive factor as assumed by Prophet, rather it grows with the trend. This is multiplicative seasonality. seasonality as a percent of the trend:
        c_prophet = Prophet(growth='logistic', holidays=holidays, seasonality_mode='multiplicative')
        print("--- Fit ---\n{}".format(df.head()))
        #print("Holidays={}".format(holidays))
        c_prophet.fit(df)
        # Make a future dataframe
        #c_forecast = c_prophet.make_future_dataframe(periods=future_days*24, freq='H')
        c_forecast = c_prophet.make_future_dataframe(periods=future_days, freq='D')
        if 'cap' in df.columns: # always true, otherwsie, logistic model will not work
           c_forecast['cap'] = df['cap']
           lastIdx           = df.shape[0]
           lastV             = df['cap'].iloc[-1]
           c_forecast.iloc[lastIdx:,1]=lastV
        else:
           c_forecast['cap'] = 18568 * 3600

        # Make predictions
        print("--- Predict ---\n{}".format(df.head()))
        c_forecast = c_prophet.predict(c_forecast)
        return c_prophet, c_forecast

    def validation (c_prophet, data_days, horizon_days=14):
        return None

        #TODO: WARNING:fbprophet:Seasonality has period of 365.25 days which is larger than initial window. Consider increasing initial.
        initial_days = min (200, int(data_days/2))
        print("validation data_days={},initial={},horizon_days={}".format(data_days,initial_days,horizon_days))
        
        # cross validation, 
        # By default, the initial training period is set to three times the horizon, and cutoffs (period) are made every half a horizon.
        #ca_cv = cross_validation(c_prophet, initial='200 days', horizon = '14 days', period='100 days') 
        print("--- Validation ---\n{}".format(df.head()))
        ca_cv = cross_validation(c_prophet, initial='{} days'.format(initial_days), horizon = '{} days'.format(horizon_days)) 
        #mean squared error (MSE), root mean squared error (RMSE), mean absolute error (MAE), mean absolute percent error (MAPE
        #ca_pm = performance_metrics(ca_cv)
        return ca_cv

    def sav_figure (name, c_prophet, c_forecast, ca_cv=None):
        fig = c_prophet.plot(c_forecast, xlabel = 'Date', ylabel = 'CPU Secs')
        a   = add_changepoints_to_plot(fig.gca(), c_prophet, c_forecast)
        plt.title  ('CPU Seconds (hourly)')
        plt.savefig(IMG_DIR + name + '_forecast.png')
        #plt.show()

        figC = c_prophet.plot_components(c_forecast)
        plt.savefig(IMG_DIR + name + '_forecastComp.png')
        #plt.show()

        if ca_cv:
           figV = plot_cross_validation_metric(ca_cv, metric='mape')
           plt.title  ('CPU Seconds Forecast MAPE (mean absolute percent error)')
           plt.savefig(IMG_DIR + name + '_forecastCV.png')

def daily (clusters=['slurm', 'slurm_plus']):
    for cluster in clusters:
       print("cluseter: {}".format(cluster))
       fname1 = "{}/{}_day_{}".format(CSV_DIR, cluster, "cpuAllocDF.csv")
       fname2 = "{}/{}_day_{}".format(CSV_DIR, cluster, "cpuAllocDF_*.csv")
       inst   = MyProphet(years=3)                      # use up to 2 years of hisotry
       print("\tcluster usage file: {}".format(fname1))
       inst.clusterUsage_forecast (fname1, days=14)   # predict 2 weeks
       print("\taccount usage file: {}".format(fname1))
       inst.accountUsage_forecast (fname2, days=14)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Make prediction of cluster and account usage.')
    parser.add_argument('-y', '--years',     type=int, default=3,   help='years of history used to make prediction')
    parser.add_argument('-d', '--future',    type=int, default=30,  help='days in the future to predict')
    parser.add_argument('-c', '--clusters', nargs="+", default=['slurm'],   help='clusters')
    args = parser.parse_args()
    daily(args.clusters)
