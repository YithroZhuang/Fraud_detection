#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sys
import pandas as pd
from genMetaPaths import MetaPathGenerator
from tqdm import *

class DataPrepare:
    def __init__(self):
        self.data = pd.DataFrame()
        self.offer2index = dict()
        self.client2index = dict()
        self.gtype = gtype
        self.clients = []
        self.partners = []
        self.offers = []
        
    
    def load_data(self, dirpath):
        self.data = pd.read_csv(dirpath, low_memory=False)
        
        
    def index_col_tripartite(self, despath):
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
    
    
    def pair_col_tripartite(self, despath):
        f1 = open(despath + '/partner_client.txt', 'w')
        f2 = open(despath + '/partner_offer.txt', 'w')
        indexs = self.data.index
        for ind in tqdm(range(len(indexs)), desc='Pair_col_writing'):
            f1.write(str(self.data.loc[indexs[ind], 'partnerid']) + '\t' + str(self.client2index[self.data.loc[indexs[ind], 'client']]) + '\n')
            f2.write(str(self.data.loc[indexs[ind], 'partnerid']) + '\t' + str(self.offer2index[self.data.loc[indexs[ind], 'cid']]) + '\n')
        
        f1.close()
        f2.close()
        
    
    def hierarchy_col_bipartite(self, despath):
        f1 = open(despath + '/client_offer.txt', 'w')
        f2 = open(despath + '/partner_client.txt', 'w')
        indexs = self.data.index
        for ind in tqdm(range(len(indexs)), desc='pair_column_writing'):
            f1.write(str(self.offer2index[self.data.loc[indexs[ind], 'client']]) + '\t' + str(self.client2index[self.data.loc[indexs[ind], 'client']]) + '\n')
            f2.write(str(self.data.loc[indexs[ind], 'partnerid']) + '\t' + str(self.offer2index[self.data.loc[indexs[ind], 'cid']]) + '\n')
        
        f1.close()
        f2.close()

    
    def index_col_bipartite(self, despath):
        if self.data['agent'].isnull().any():
            t = self.data['agent']
            t = t.fillna('other')
            self.data['agent'] = t
        self.data['client'] = self.data['agent'] + self.data['iplong'].astyepe(str)
        self.clients = self.data['client'].unique().tolist()
        with open(despath + 'id_client.txt', 'w') as f:
            for index, cc in enumerate(self.clients):
                f.write(str(index) + '\t' + str(cc) + '\n')
                self.client2index[cc] = index
        
        self.partners = self.data['partnerid'].unique().tolist()                
    
    
    def pair_col_bipartite(self, despath):
        indexs = self.data.index
        with open(despath + '/partner_client.txt', 'w') as f:
            for ind in tqdm(range(len(indexs)), desc='Pair_col_bipartite_writing'):
                f.write(str(self.data.loc[indexs[ind], 'partnerid']) + '\t' + str(self.client2index[self.data.loc[indexs[ind], 'client']]) + '\n')
                
    
    def tripartite(self, despath):
        self.index_col_tripartite(despath)
        self.pair_col_tripartite(despath)
         
        
    def bipartite(self, despath):
        self.index_col_bipartite(despath)
        self.pair_col_bipartite(despath)
       
    
    def hierarchy_bipartite(self, despath):
        self.index_col_tripartite(despath)
        self.hierarchy_col_bipartite(despath)
    
    def generate_metapath(self, despath, numwalks, walklength):
        self.MPG = MetaPathGenerator()
        if self.gtype == 1:
            self.MPG.read_data_tripartite(despath)
            self.MPG.generate_random_apcpa(despath+ '/random_walk.txt', numwalks, walklength)
        elif self.gtype == 2:
            self.MPG.read_data_bipartite(despath)
            self.MPG.generate_random_pcp(despath + '/random_walk.txt', numwalks, walklength)
        elif self.gtype == 3:
            self.MPG.read_data_hierarchy(despath)
            self.MPG.generate_random_pcp(despath + '/random_walk_application.txt', numwalks, walklength)
            self.MPG.generate_random_coc(despath + '/random_walk_client.txt', numwalks, walklength)
    
    
    def read_node_type(self, srcfile, node_type):
        with open(srcfile, 'w') as f:
            for line in f:
                splits = line.strip('\n').split('\t')
                for split in splits:
                    if split not in self.result_type:
                        self.result_type[split] = node_type[split]
    
    # Generate node_type file
    def generate_node_type(self, despath):
        node_type = {}
        for cc in self.partners:
            node_type[cc] = 'p'
        for cc in self.clients:
            node_type[str(cc)] = 'c'
        for cc in self.offers:
            node_type[cc] = 'o'
        # print len(node_type.keys())
        
        self.result_type = {}
        des_f = open(despath + '/node_type_mapings.txt', 'w')
        if self.gtype in [1, 2]:
            self.read_node_type(despath + 'random_walk.txt', node_type)
        else:
            self.read_node_type(despath + 'random_walk_application.txt', node_type)
            self.read_node_type(despath + 'random_walk_client.txt', node_type)
            
        for key in result_type:
            des_f.write(key + '\t' + result_type[key] + '\n')
        des_f.close()

# graph type 1 for tripatite
gtype = sys.argv[1]
# Path of original dataset
dirpath = sys.argv[2]
# Path of destination floder
despath = sys.argv[3]
# Number of walks and length of walk a nodes walks
numwalks = int(sys.argv[4])
walklength = int(sys.argv[5])

def main():
    dp = DataPrepare()
    print 'loading data...'
    dp.load_data(dirpath)
    if gtype == 1:
        print 'creating files...'
        dp.tripartite(despath)
        print 'generating metapathes...'
        dp.generate_metapath(despath, numwalks, walklength)
        print 'generating node type file...'
        dp.generate_node_type(despath)
    elif gtype == 2:
        print 'creating files...'
        dp.bipartite(despath)
        print 'generating metapathes...'
        dp.generate_metapath(despath, numwalks, walklength)
        print 'generating node type file...'
        dp.generate_node_type(despath)
    elif gtype == 3:
        print 'creating files...'
        dp.hierarchy_bipartite(despath)
        print 'generating metapathes...'
        dp.generate_metapath(despath, numwalks, walklength)
        

if __name__ == '__main__':
    main()
            
