#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
from genMetaPaths import MetaPathGenerator
from tqdm import *

class DataPrepare:
    def __init__(self):
        self.data = pd.DataFrame()
        self.offer2index = dict()
        self.client2index = dict()
        
    
    def load_data(self, dirpath):
        self.data = pd.read_csv(dirpath, low_memory=False)
        
        
    def index_col(self, despath):
        if self.data['cid'].isnull().any():
            t = self.data['cid']
            t = t.fillna('other')
            self.data['cid'] = t
        self.offers = self.data['cid'].unique().tolist()
        with open(despath + '/id_offer.txt', 'w') as f:
            for index, cc in enumerate(self.offers):
                f.write(str(index) + '\t' + str(cc) + '\n')
                self.offer2index[cc] = index
        
        if self.data['agent'].isnull().any():
            t = self.data['agent']
            t = t.fillna('other')
            self.data['agent'] = t
        self.data['client'] = self.data['agent'] + self.data['iplong'].astype(str)
        self.clients = self.data['client']
        with open(despath + '/id_client.txt', 'w') as f:
            for index, cc in enumerate(self.clients):
                f.write(str(index) + '\t' + str(cc) + '\n')
                self.client2index[cc] = index
        
        self.partners = self.data['partnerid'].unique().tolist()
        with open(despath + '/partner.txt', 'w') as f:
            for cc in self.partners:
                f.write(str(cc) + '\n')
        
        
    def pair_col(self, despath):
        f1 = open(despath + '/partner_client.txt', 'w')
        f2 = open(despath + '/partner_offer.txt', 'w')
        indexs = self.data.index
        for ind in tqdm(range(len(indexs)), desc='Pair_col_writing'):
            f1.write(str(self.data.loc[indexs[ind], 'partnerid']) + '\t' + str(self.client2index[self.data.loc[indexs[ind], 'client']]) + '\n')
            f2.write(str(self.data.loc[indexs[ind], 'partnerid']) + '\t' + str(self.offer2index[self.data.loc[indexs[ind], 'cid']]) + '\n')
        
        f1.close()
        f2.close()
       
    
    def generate_metapath(self, despath, numwalks, walklength):
        self.MPG = MetaPathGenerator()
        self.MPG.read_data(despath)
        self.MPG.generate_random_apcpa(despath+ '/random_walk.txt', numwalks, walklength)
        
    # Generate node_type file
    def generate_node_type(self, despath):
        node_type = {}
        for cc in self.partners:
            node_type[cc] = 'p'
        for cc in self.clients:
            node_type[cc] = 'c'
        for cc in self.offers:
            node_type[cc] = 'o'
        # print len(node_type.keys())
            
        des_f = open(despath + '/node_type_mapings.txt', 'w')
        with open(despath + '/random_walk.txt', 'r') as f:
            for line in f:
                splits = line.strip().split('\t')
                for split in splits:
                    try:
                        des_f.write(split + '\t' + node_type[split] + '\n')
                    except KeyError:
                        print split
        
        des_f.close()
        
# Path of original dataset
dirpath = sys.argv[1]
# Path of destination floder
despath = sys.argv[2]
# Number of walks and length of walk a nodes walks
numwalks = int(sys.argv[3])
walklength = int(sys.argv[4])

def main():
    dp = DataPrepare()
    print 'loading data...'
    dp.load_data(dirpath)
    print 'creating index files...'
    dp.index_col(despath)
    print 'creating pair-wise files...'
    dp.pair_col(despath)
    print 'generating metapaths...'
    dp.generate_metapath(despath, numwalks, walklength)
    print 'generating node type file...'
    dp.generate_node_type(despath)
    

if __name__ == '__main__':
    main()
            