#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 11:12:54 2018

@author: yithro
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
import datetime
import time
from multiprocessing import Pool, Process

"""
Class of construct statistic feature according to paper of FDMA2012

"Detecting Click Fraud in Online Advertising: A Data Mining Approach"
"""

class statistic_feature:
    def __init__(self, path, dtype, filename, columns_values):
        self.data = pd.DataFrame()
        self.path = path
        self.dtype = dtype
        self.filename = filename
        self.columns_values = columns_values
        self.night = range(0, 6)
        self.morning = range(6, 12)
        self.afternoon = range(12, 18)
        self.evening = range(18, 23)
        self.second_15_minitue = range(16, 30)
        self.sub_features = []
        self.sub_data = []
        self._load_data()
        self.partners = self._time_format()
    
    def _load_data(self):
        self.data = pd.read_csv(self.path + '/' + self.filename, low_memory=False)
        
        
    def _time_format(self):
        times = self.data['timeat'].tolist()
        tmp_times = []
        for t in times:
            try:
                tmp_times.append(datetime.datetime.strptime(t[:-2], '%Y-%m-%d %H:%M:%S'))
            except ValueError:
                print t
        
        day, hour, minute = [], [], []
        for i in range(len(tmp_times)):
            tmp = tmp_times[i].timetuple()
            day.append(tmp.tm_mday)
            hour.append(tmp.tm_hour)
            minute.append(tmp.tm_min)
            
        self.data['day'] = day
        self.data['hour'] = hour
        self.data['minute'] = minute
        self.data['per_hour'] = self.data['day'] * 10 + self.data['hour']
        
        self.data = self.data.sort_values(['partnerid', 'day', 'hour', 'minute'], ascending=True)
        
        partner = self.data['partnerid'].unique().tolist()
        return partner
        
    
    def _construct_features(self, partner, dataframe, dtype, index):
        start = time.time()
        my_result = {}
        for cv in self.columns_values:
            my_result[cv] = []
            
        for ind in tqdm(range(len(partner))):
            tmp = dataframe[dataframe['partnerid']==partner[ind]]
            total_click = tmp.shape[0]
            my_result[cols[0]].append(total_click)
            t = tmp['referer']
            t = t.fillna('others')
            tmp['referer'] = t
            dis_ref = tmp['referer'].nunique()
            my_result[cols[1]].append(dis_ref)
            my_result[cols[2]].append(tmp[tmp['hour'].isin(self.night)]['referer'].nunique() * 1.0 / dis_ref)
            my_result[cols[3]].append(tmp[tmp['minute'].isin(self.second_15_minitue)].shape[0] * 1.0 / total_click)
            my_result[cols[4]].append(tmp[tmp['hour'].isin(self.morning)].shape[0] * 1.0 / total_click)
            my_result[cols[5]].append(tmp[tmp['cntr']=='id'].shape[0] * 1.0 / total_click)
            my_result[cols[6]].append(tmp[tmp['cntr']=='sg'].shape[0] * 1.0 / total_click)
            my_result[cols[7]].append(self._std_per_hour_density(tmp))
            re = self._minute_level_features(tmp)
            for i in range(1,len(cols)-7):
                my_result[cols[7+i]].append(re[i-1])
        result_df = pd.DataFrame(my_result)
        result_df['partnerid'] = partner
        self.sub_features.append(result_df)
        result_df.to_csv(self.path + '/' + dtype + '/feature_' + str(index) + '.csv', index=False)
        end = time.time()
        print'time:' + str(end-start)
        
        
    def _std_per_hour_density(self, data):
        time = data['per_hour'].unique().tolist()
        count = []
        for t in time:
            count.append(data[data['per_hour']==t].shape[0])
        count = np.array(count)
        return np.std(count)
    
    # Features of one minute level
    def _minute_level_features(self, data):
        minute = data['minute'].unique().tolist()
        referred = []
        click = []
        numericip = []
        numericurl = []
        reagcnipci = []
        agent = []
        after_agent = []
        reagcn = []
        night_reagcnipci = []
        after_reagcnipci = []
        night_referred = []
        for m in minute:
            tmp = data[data['minute']==m]
            
            after_tmp = tmp[tmp['hour'].isin(self.afternoon)]
            night_tmp = tmp[tmp['hour'].isin(self.night)]
            
            # Distinct referredurl in one miniute
            referred.append(tmp['referer'].nunique())
            
            # Total clicks in one minitu
            click.append(tmp.shape[0])
            
            # Count ip being duplicated in one minute 
            num_ip = self.count_Single('iplong', tmp)
            numericip.append(num_ip)
            
            # Count referredurl being duplicated in one minute
            num_url = self.count_Single('referer', tmp)
            numericurl.append(num_url)
            
            # Count referredurl being duplicated at night in one minute
            num_url_night = self.count_Single('referer', night_tmp)
            night_referred.append(num_url_night)
            
            # Count agent being duplicated in one minute
            num_agent = self.count_Single('agent', tmp)
            agent.append(num_agent)
            
            # Count agent being duplicated in the afternoon in one minute
            num_agent_after = self.count_Single('agent', after_tmp)
            after_agent.append(num_agent_after)
            
            # Count multi cols being duplicated in one minute 
            if num_url == 0:
                reagcn.append(0)
                reagcnipci.append(0)
                after_reagcnipci.append(0)
                night_reagcnipci.append(0)
            else:
                reagcn.append(self.count_Multi('referer', tmp[['referer', 'agent', 'cntr']]))
                reagcnipci.append(self.count_Multi('referer', tmp[['referer', 'iplong', 'cid', 'agent', 'cntr']]))
                after_reagcnipci.append(self.count_Multi('referer', \
                                                    after_tmp[['referer', 'iplong', 'cid', 'agent', 'cntr']]))
                night_reagcnipci.append(self.count_Multi('referer', \
                                                    night_tmp[['referer', 'iplong', 'cid', 'agent', 'cntr']]))
            
        
        referred, click, numericip, numericurl, reagcnipci = \
                    np.array(referred), np.array(click), np.array(numericip), np.array(numericurl), np.array(reagcnipci)
        
        agent, after_agent, reagcn, night_reagcnipci, after_reagcnipci, night_referred = \
              np.array(agent), np.array(after_agent), np.array(reagcn), np.array(night_reagcnipci), \
              np.array(after_reagcnipci), np.array(night_referred)
        
        result = [np.mean(referred), np.std(click), np.std(referred), np.std(numericip), np.mean(reagcnipci), 
                  np.mean(agent), np.mean(numericurl), np.std(numericurl), np.mean(reagcn), np.mean(night_reagcnipci),
                  np.mean(after_reagcnipci), np.mean(after_agent), np.mean(night_referred)]
        
        return result
    
    # Count single col being duplicated in one minute
    def _count_Single(self, col, data):
        count = 0
        vals = data[col].unique().tolist()
        for val in vals:
            if data[data[col]==val].shape[0] > 1:
                count += 1
        return count
        
    
    # Count same referredur, ip, cid, agent, cntr being duplicated in one minute
    def _count_Multi(self, col, data):
        count = 0
        vals = data[col].unique().tolist()
        for val in vals:
            tmp = data[data[col]==val]
            tmp2 = tmp.drop_duplicates()
            if tmp.shape[0] != tmp2.shape[0]:
                count += 1
        return count
    
    
    def _multi_process_construct(self, num_process):
        count = len(self.partners) / 600
        sub_partner, sub_data = [], []
        for i in range(count):
            sub_partner.append(self.partners[i*600:(i+1)*600])
        sub_partner.append(self.partners[count*600:])
        
        pool = Pool(processes=num_process)
        
        for i in range(len(sub_partner)):
            sub_data = self.data[self.data['partnerid'].isin(sub_partner[i])]
            pool.apply_async(func=self._construct_features, args=(sub_partner[i],sub_data, self.dtype, i+1))
        
        pool.close()
        pool.join()
        
        print 'Sub_processes done.'
        

if __name__ == '__main__':
    path = '/home/yithro/workplace/data'
    cols = ['total_clicks', 'distinct_referredurl', 'night_referredurl_percent', 'second_15_minute_percent', 
        'morning_click_percent', 'usercountry_id_percent', 'usercountry_sg_percent','std_per_hour_density',
        'avg_distinct_referredurl', 'std_total_clicks', 'std_distinct_referredurl', 'std_spiky_numericip', 
        'avg_spiky_ReAgCnIpCi', 'avg_spiky_deviceua', 'avg_spiky_referredurl', 'std_spiky_referredurl',
        'avg_spiky_ReAgCn', 'night_avg_spiky_ReAgCnIpCi', 'afternoon_avg_ReAgCnIpCi', 'afternoon_avg_spiky_deviceua',
        'night_avg_spiky_referredurl']
    st = statistic_feature(path, 'train', 'clicks_09feb12.csv', cols)
    st._multi_process_construct(2)
    print len(st.sub_features)
        