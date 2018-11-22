#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import numpy as np
import tensorflow as tf
import json
from utils import get_optimizer

class Skipgram:
    def __init__(self, VOCAB_SIZE, EMBED_SIZE, NUM_SAMPLED, LEARNING_RATE, BATCH_SIZE, OPT_ALGO, DATASET, LOG_DIRECTORY):
        self.vocab_size = VOCAB_SIZE
        self.embed_size = EMBED_SIZE
        self.num_samples = NUM_SAMPLED
        self.learning_rate = LEARNING_RATE
        self.batch_size = BATCH_SIZE
        self.opt_algo = OPT_ALGO
        self.dataset = DATASET
        self.datasize = self.dataset.node_context_pairs[0].shape[0]
        self.log_directory = LOG_DIRECTORY
        self._build_model()
        
    def _build_model(self):
        
        self.graph = tf.Graph()
        with tf.name_scope('data'):
            self.center_node = tf.placeholder(tf.int32, shape=[None], name='center_node')
            self.context_node = tf.placeholder(tf.int32, shape=[None, 1], name='context_node')
            self.negative_samples = (tf.placeholder(tf.int32, shape=[self.num_samples], name='negative_samples'),
                                tf.placeholder(tf.float32, shape=[None, 1], name='true_expected_count'),
                                tf.placeholder(tf.float32, shape=[self.num_samples], name='sampled_expected_count'))
        
        with tf.name_scope('embedding_matrix'):
            self.embed_matrix = tf.Variable(tf.random_uniform([self.vocab_size, self.embed_size], 0.0, 1.0), 
                                    name='embed_matrix')
            tf.summary.histogram('embedding_matrix', self.embed_matrix)
        
            # define the inference
        with tf.name_scope('loss'):
            embed = tf.nn.embedding_lookup(self.embed_matrix, self.center_node, name='embed')
        
            #construct variables for NCE loss
            nce_weight = tf.Variable(tf.truncated_normal([self.vocab_size, self.embed_size],
                                                            stddev=1.0 / (self.embed_size ** 0.5)), 
                                                            name='nce_weight')
            nce_bias = tf.Variable(tf.zeros([self.vocab_size]), name='nce_bias')
        
                # define loss function to be NCE loss function
            self.loss = tf.reduce_mean(tf.nn.nce_loss(weights=nce_weight, 
                                                    biases=nce_bias, 
                                                    labels=self.context_node, 
                                                    inputs=embed,
                                                    sampled_values = self.negative_samples, 
                                                    num_sampled=self.num_samples, 
                                                    num_classes=self.vocab_size), name='loss')
        
            tf.summary.scalar("loss_summary", self.loss)
            
        global_step = tf.Variable(0.0)
        with tf.name_scope('learning_rate'):
            self.learning_rate = tf.train.exponential_decay(self.learning_rate, global_step, float(self.datasize)/self.batch_size, 0.98)
            tf.summary.scalar('Learning_rate', self.learning_rate)
            
        #define optimizer
        with tf.name_scope('optimizer'):
            self.optimizer = get_optimizer(self.opt_algo, self.learning_rate, self.loss, global_step)
        self.merge = tf.summary.merge_all()
        self.config = tf.ConfigProto()
        self.config.gpu_options.allow_growth = True
        self.sess = tf.Session(config=self.config)
            
            
        tf.global_variables_initializer().run(session=self.sess)


    def _train(self, epochs, care_type):
        '''
        tensorflow training loop
        define SGD trining
        *epoch index starts from 1! not 0.
        '''
        care_type = True if care_type==1 else False
        
        # Add ops to save and restore all the variables.
        # saver = tf.train.Saver(max_to_keep=max_keep_model)
        writer = tf.summary.FileWriter(self.log_directory, self.graph)
        
        
        data = tf.data.Dataset.from_tensor_slices((self.center_node, self.context_node))
        data = data.shuffle(10000).batch(self.batch_size)
        
        iterator = data.make_initializable_iterator()
        next_element = iterator.get_next()
        
        for epoch in range(1, epochs+1):
            fetch = [self.optimizer, self.loss, self.merge]
            total_loss = 0.0
            iteration = 0
            self.sess.run(iterator.initializer, feed_dict={self.center_node:self.dataset.node_context_pairs[0],
                                                           self.context_node:self.dataset.node_context_pairs[1].reshape(-1,1)})
            
            while True:
                try:
                    center_node_batch, context_node_batch = self.sess.run(next_element)
                    negative_samples  = self.dataset.get_negative_samples(pos_index=context_node_batch,num_negatives=self.num_samples,care_type=care_type)
                    context_node_batch = context_node_batch.reshape((-1,1))
                    _, loss_batch,summary_str = self.sess.run(fetch, feed_dict={self.center_node:center_node_batch,
                                                                                self.context_node:context_node_batch,
                                                                                self.negative_samples:negative_samples
                                                                                })
                    writer.add_summary(summary_str,iteration)
                    total_loss += loss_batch
    
                    # print(loss_batch)
    
                    iteration+=1
                except tf.errors.OutOfRangeError:
                    print('Epoch {:3d}, loss={:.5f}'.format(epoch, total_loss/self.datasize))
                    break

            writer.close()
    
        print("Save final embeddings as numpy array")
        np_node_embeddings = tf.get_default_graph().get_tensor_by_name("embedding_matrix/embed_matrix:0")
        np_node_embeddings = self.sess.run(np_node_embeddings)
        np.savez(os.path.join(self.log_directory,"node_embeddings.npz"),np_node_embeddings)
    
        with open(os.path.join(self.log_directory,"index2nodeid.json"), 'w') as f:  
            json.dump(self.dataset.index2nodeid, f, sort_keys=True, indent=4)  
