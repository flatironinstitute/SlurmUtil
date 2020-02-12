import datetime
import pandas as pd
import fbprophet as fbp
import matplotlib.pyplot as plt
from querySlurm import SlurmDBQuery
from fbprophet.plot import add_changepoints_to_plot
from fbprophet.plot import plot_cross_validation_metric
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics

class MyProphet:

    def __init__(self):
        self.holidays = self.getHolidays()
        self.querySDBClient = SlurmDBQuery()
 
    def getHolidays(self):
        names = ['New Years Day', 'Martin Luther King, Jr. Day', 'Washington’s Birthday',
             'Good Friday',   'Memorial Day',                'Independence Day', 
             'Labor Day',     'Thanksgiving Day',            
             'Christmas']
        date  = [['2018-01-01','2019-01-01'], ['2018-01-15','2019-01-21'], ['2018-02-19','2019-02-18'],
             ['2018-03-30','2019-04-19'], ['2018-05-25','2018-05-28','2019-05-24','2019-05-27'], ['2018-07-04','2019-07-04'],
             ['2018-09-03','2019-09-02'], ['2017-11-23','2017-11-24','2018-11-22','2018-11-23','2019-11-28','2019-11-29'], 
             ['2017-12-25','2017-12-26','2017-12-27','2017-12-28','2017-12-29', 
              '2018-12-24','2018-12-25','2018-12-26','2018-12-27','2018-12-28',
              '2019-12-25']]

        frames = []
        for idx in range(len(names)):
            df = pd.DataFrame({'holiday': names[idx], 'ds': pd.to_datetime(date[idx])})
            frames.append (df)

        return pd.concat(frames)
       
    def accountUsage_forecast (self, cps=0.5, p=720):
        #df index ['id_tres','acct', 'time_start'], 
        start, stop, df = self.querySDBClient.getAccountUsage_hourly()
        df['ds']        = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))

        #prepare data
        for acct in ['cca', 'ccb', 'ccq']:
            # get cpu,account data
            cpuDf = df.loc[(1,acct,),]
            cpuAllocDf = cpuDf[['ds', 'alloc_secs']]
            cpuAllocDf = cpuAllocDf.rename(columns={'alloc_secs': 'y'})
            cpuAllocDf['cap'] = 18568 * 3600
        
            self.analyze(cpuAllocDf, acct, cps, p)

    def analyze (self, df, name, cps, p):    
        holidays   = self.getHolidays()
        # Make the prophet model and fit on the data
        #c_prophet = fbp.Prophet(changepoint_prior_scale=cps)
        c_prophet = fbp.Prophet(growth='logistic', holidays=holidays, seasonality_mode='multiplicative')
        c_prophet.fit(df)

        # Make a future dataframe for 2 years
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

        fig = c_prophet.plot(c_forecast, xlabel = 'Date', ylabel = 'CPU Secs')
        a   = add_changepoints_to_plot(fig.gca(), c_prophet, c_forecast)
        plt.title  ('CPU Seconds (hourly)')
        plt.savefig('./public/images/' + name + 'Forecast.png')
        #plt.show()

        figC = c_prophet.plot_components(c_forecast)
        plt.savefig('./public/images/' + name + 'ForecastComp.png')
        #plt.show()

        # cross validation
        ca_cv = cross_validation(c_prophet, initial='200 days', horizon = '14 days', period='100 days') 
        #mean squared error (MSE), root mean squared error (RMSE), mean absolute error (MAE), mean absolute percent error (MAPE
        #ca_pm = performance_metrics(ca_cv)
        figV = plot_cross_validation_metric(ca_cv, metric='mape')
        plt.title  ('CPU Seconds Forecast MAPE (mean absolute percent error)')
        plt.savefig('./public/images/' + name + 'ForecastCV.png')

    def clusterUsage_forecast (self, cps=0.5, p=720):
        df               = pd.read_csv("slurm_cluster_usage_hour_table.csv", names=['creation_time','mod_time','deleted','id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'], usecols=['id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'])
        df['tdown_secs'] = df['down_secs']+df['pdown_secs']
        df['total_secs'] = df['alloc_secs']+df['down_secs']+df['pdown_secs']+df['idle_secs']+df['resv_secs']
        df               = df[df['count'] * 3600 == df['total_secs']]
        df['ds']         = df['time_start'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

        # get cpu data only
        dfg        = df.groupby  ('id_tres')
        cpuDf      = dfg.get_group(1)
        cpuDf.reset_index (drop=True, inplace=True)

        # remove 0 alloc_secs at the beginning 
        idx        = 0
        while cpuDf.loc[idx,'alloc_secs'] == 0:
            idx += 1
        cpuDf.drop(range(0, idx), inplace=True)
        cpuDf.reset_index (drop=True, inplace=True)

        #prepare data
        cpuAllocDf = cpuDf[['ds', 'alloc_secs', 'total_secs']]
        cpuAllocDf = cpuAllocDf.rename(columns={'alloc_secs': 'y', 'total_secs': 'cap'})
        self.analyze (cpuAllocDf, 'cluster', cps, p)

        cpuDownDf  = cpuDf[['ds', 'tdown_secs']]
        cpuDownDf  = cpuDownDf.rename(columns={'tdown_secs': 'y'})
        cpuDownDf['cap'] = 18568 * 3600
        self.analyze (cpuDownDf, 'clusterDown', cps, p)

def main():
    inst = MyProphet()
    inst.clusterUsage_forecast ()
    inst.accountUsage_forecast ()
if __name__=="__main__":
   main()
