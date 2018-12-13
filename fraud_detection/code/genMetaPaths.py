#! /usr/bin/env python2
# -*- coding: utf-8 -*-

import sys
import os
import random
from collections import Counter


class MetaPathGenerator:
    def __init__(self):
        self.id_client = dict()
        self.id_offer = dict()
        self.partner_client = dict()
        self.partner_offer = dict()
        self.client_partner = dict()
        self.offer_partner = dict()
        self.client_offer = dict()
        self.offer_client = dict()
	self.target_client = set()
        # self.cid_cocidlist = dict()
        # self.agent_cidlist = dict()
        # self.cid_agentlist = dict()

    # Load data for tripartite graph
    def read_data_tripartite(self, dirpath):

        # Load user's information
        with open(dirpath + '/id_client.txt', 'r') as uf:
            for line in uf:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    self.id_client[toks[0]] = toks[1].replace(" ", "")

        # Load offer's information
        with open(dirpath + '/id_offer.txt', 'r') as of:
            for line in of:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    self.id_offer[toks[0]] = toks[1].replace(" ", "")

        # Load traffic_user information
        with open(dirpath + '/partner_client.txt', 'r') as tuf:
            for line in tuf:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    p, c = toks[0], toks[1]
                    if p not in self.partner_client:
                        self.partner_client[p] = []
                    self.partner_client[p].append(c)
                    if c not in self.client_partner:
                        self.client_partner[c] = []
                    self.client_partner[c].append(p)

        # Load traffic_offer information
        with open(dirpath + '/partner_offer.txt', 'r') as tof:
            for line in tof:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    p, o = toks[0], toks[1]
                    if p not in self.partner_offer:
                        self.partner_offer[p] = []
                    self.partner_offer[p].append(o)
                    if o not in self.offer_partner:
                        self.offer_partner[o] = []
                    self.offer_partner[o].append(p) 
    
    # Load data for bipartite graoh
    def read_data_bipartite(self, dirpath):
        
        # load user's information
        with open(dirpath + '/id_client.txt', 'r') as uf:
            for line in uf:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    self.id_client[toks[0]] = toks[1].replace(" ", "")
        
        # load application_user information
        with open(dirpath + '/partner_client.txt', 'r') as tuf:
            for line in tuf:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    p, c = toks[0], toks[1]
                    if p not in self.partner_client:
                        self.partner_client[p] = []
                    self.partner_client[p].append(c)
                    if c not in self.client_partner:
                        self.client_partner[c] = []
                    self.client_partner[c].append(p)

    
    
    # load data for hierarchy bipartite
    def read_data_hierarchy(self, dirpath):
        
        # Load users' information
        with open(dirpath + '/id_client.txt', 'r') as uf:
            for line in uf:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    self.id_client[toks[0]] = toks[1].replace(" ", "")
        
        # Load offers' information
        with open(dirpath + '/id_offer.txt', 'r') as of:
            for line in of:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    self.id_offer[toks[0]] = toks[1].replace(" ", "")
                    
        # Load application_user information
        with open(dirpath + '/partner_client.txt', 'r') as tuf:
            for line in tuf:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    p, c = toks[0], toks[1]
                    if p not in self.partner_client:
                        self.partner_client[p] = []
                    self.partner_client[p].append(c)
                    if c not in self.client_partner:
                        self.client_partner[c] = []
                    self.client_partner[c].append(p)
        
        # Load client_advertisment information
        with open(dirpath + '/client_offer.txt', 'r') as cof:
            for line in cof:
                toks = line.strip('\n').split('\t')
                if len(toks) == 2:
                    c, o = toks[0], toks[1]
                    if c not in self.client_offer:
                        self.client_offer[c] = []
                    self.client_offer[c].append(o)
                    if o not in self.offer_client:
                        self.offer_client[o] = []
                    self.offer_client[o].append(c)
            
    # Generate random walk path apcpa
    def generate_random_apcpa(self, outfilename, numwalks, walklenghth):

        with open(outfilename, 'w') as outfile:
            for c in self.client_partner:
                c0 = c
                for i in xrange(0, numwalks):
                    outline = self.id_client[c0]
                    for j in xrange(0, walklenghth):
                        p1 = self.client_partner[c0]
                        nump1 = len(p1)
                        pid1 = random.randrange(nump1)
                        p1 = p1[pid1]
                        outline += '\t' + p1
                        o = self.partner_offer[p1]
                        numc = len(o)
                        oid = random.randrange(numc)
                        o = o[oid]
                        outline += '\t' + self.id_offer[o]
                        p2 = self.offer_partner[o]
                        nump2 = len(p2)
                        pid2 = random.randrange(nump2)
                        p2 = p2[pid2]
                        outline += '\t' + p2
                        c1 = self.partner_client[p2]
                        numa = len(c1)
                        cid = random.randrange(numa)
                        c1 = c1[cid]
                        outline += '\t' + self.id_client[c1]
                    outfile.write(outline + '\n')
                    
    # Generate randon walk path pcp
    def generate_random_pcp(self, outfilename, numwalks, walklength):
        with open(outfilename, 'w') as outfile:
            for p in self.partner_client:
                p0 = p
                for i in xrange(0, numwalks):
                    outline = p0
                    for j in xrange(0, walklength):
                        c = self.partner_client[p0]
                        numc = len(c)
                        cid = random.randrange(numc)
                        c0 = c[cid]
			self.target_client.add(c0)
                        outline += '\t' + self.id_client[c0]
                        p1 = self.client_partner[c0]
                        nump = len(p1)
                        pid = random.randrange(nump)
                        p1 = p1[pid]
                        outline += '\t' + p1
                    outfile.write(outline + '\n')
                    
    # Generate random walk path coc
    def generate_random_coc(self, outfilename, numwalks, walklength):
        with open(outfilename, 'w') as outfile:
	    self.target_client = list(self.target_client)
            for c in self.target_client:
                c0 = c
                for i in xrange(0, numwalks):
                    outline = self.id_client[c0]
                    for j in xrange(0, walklength):
                        o = self.client_offer[c0]
                        numo = len(o)
                        oid = random.randrange(numo)
                        o0 = o[oid]
                        outline += '\t' + self.id_offer[o0]
                        c1 = self.offer_client[o0]
                        numc = len(c1)
                        cid = random.randrange(numc)
                        c1 = c1[cid]
                        outline += '\t' + self.id_client[c1]
                    outfile.write(outline + '\n')
                    
# =============================================================================
# if __name__ == '__main__':
#     MPG = MetaPathGenerator()
#     MPG.read_data('/data/zy/youmi/FDMA/metapath/train')
#     MPG.generate_random_apcpa('/data/zy/youmi/FDMA/metapath/train/random_walk.txt', 1, 1)
# 
# =============================================================================
