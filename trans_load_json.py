# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 16:52:27 2018

@author: shbhat
"""

import pandas as pd 
import pickle 
import pprint
import json
import random


def read_data_sample(filename):
    #n = sum(1 for line in open(filename)) - 1 #number of records in file (excludes header)
    #skip = sorted(random.sample(xrange(1,n+1),n-s)) #the 0-indexed header will not be included in the skip list
    data = pd.read_csv(filename)
    return data


def create_vertex_struct():
    json_struct=dict()
    json_struct['source']={}
    json_struct['source']['file']={}
    json_struct['source']['file']['path']='add-path'
    json_struct['extractor']={}
    json_struct['extractor']['csv']={}
    json_struct['transformers']=[]
    json_struct['transformers'].append({})
    json_struct['transformers'][0]['vertex']={}
    json_struct['transformers'][0]['vertex']['class']='class_name'  
    json_struct['loader']={}
    json_struct['loader']['orientdb']={}
    json_struct['loader']['orientdb']['dbURL']='add-URL'
    json_struct['loader']['orientdb']['dbType']='add_type'
    json_struct['loader']['orientdb']['classes']=[]
    json_struct['loader']['orientdb']['classes'].append({})
    json_struct['loader']['orientdb']['classes'][0]['name']='class_name'
    json_struct['loader']['orientdb']['classes'][0]['extends']='superclass'
    json_struct['loader']['orientdb']['indexes']=[]
    json_struct['loader']['orientdb']['indexes'].append({})
    json_struct['loader']['orientdb']['indexes'][0]['class']='class_name'
    json_struct['loader']['orientdb']['indexes'][0]['fields']='index_names'
    json_struct['loader']['orientdb']['indexes'][0]['type']='type_field'
    return json_struct
    

    
def create_vertex_edge_struct():
    json_struct=dict()
    json_struct['source']={}
    json_struct['source']['file']={}
    json_struct['source']['file']['path']='add-path'
    json_struct['extractor']={}
    json_struct['extractor']['csv']={}
    json_struct['transformers']=[]
    json_struct['transformers'].append({})
    json_struct['transformers'][0]['vertex']={}
    json_struct['transformers'][0]['vertex']['class']='vertex_class_name'  
    json_struct['transformers'].append({})
    json_struct['transformers'][1]['edge']={}
    json_struct['transformers'][1]['edge']['class']='edge_class_name'
    json_struct['transformers'][1]['edge']['joinFieldName']="join_id"
    json_struct['transformers'][1]['edge']['lookup']='lookup_id'    
    json_struct['loader']={}
    json_struct['loader']['orientdb']={}
    json_struct['loader']['orientdb']['dbURL']='add-URL'
    json_struct['loader']['orientdb']['dbType']='add_type'
    json_struct['loader']['orientdb']['classes']=[]
    json_struct['loader']['orientdb']['classes'].append({})
    json_struct['loader']['orientdb']['classes'][0]['name']='class_name'
    json_struct['loader']['orientdb']['classes'][0]['extends']='vertex_superclass'
    json_struct['loader']['orientdb']['classes'].append({})
    json_struct['loader']['orientdb']['classes'][1]['name']='class_name'
    json_struct['loader']['orientdb']['classes'][1]['extends']='edge_superclass'   
    json_struct['loader']['orientdb']['indexes']=[]
    json_struct['loader']['orientdb']['indexes'].append({})
    json_struct['loader']['orientdb']['indexes'][0]['class']='class_name'
    json_struct['loader']['orientdb']['indexes'][0]['fields']='index_names'
    json_struct['loader']['orientdb']['indexes'][0]['type']='type_field'
    return json_struct
    
    
def gen_vertex_config(path,superclass,class_name,db_url,db_type,index_names,type_field):
    json_struct=create_vertex_struct()
    json_struct['source']['file']['path']=path
    json_struct['transformers'][0]['vertex']['class']=class_name
    json_struct['loader']['orientdb']['dbURL']=db_url
    json_struct['loader']['orientdb']['dbType']=db_type
    json_struct['loader']['orientdb']['classes'][0]['name']=class_name
    json_struct['loader']['orientdb']['classes'][0]['extends']=superclass
    json_struct['loader']['orientdb']['indexes'][0]['class']=class_name
    json_struct['loader']['orientdb']['indexes'][0]['fields']=index_names
    json_struct['loader']['orientdb']['indexes'][0]['type']=type_field
    return json_struct
    
def gen_vertex_egde_config(path,vertex_class_name,vertex_superclass,edge_superclass,edge_class_name,join_field,edge_lookup,db_url,db_type,index_names,type_field):
    json_struct=create_vertex_edge_struct()
    json_struct['source']['file']['path']=path
    json_struct['transformers'][0]['vertex']['class']=vertex_class_name
    json_struct['transformers'][1]['edge']['class']=edge_class_name
    json_struct['transformers'][1]['edge']['joinFieldName']=join_field
    json_struct['transformers'][1]['edge']['lookup']=edge_lookup
    json_struct['loader']['orientdb']['dbURL']=db_url
    json_struct['loader']['orientdb']['dbType']=db_type
    json_struct['loader']['orientdb']['classes'][0]['name']=vertex_class_name
    json_struct['loader']['orientdb']['classes'][0]['extends']=vertex_superclass
    json_struct['loader']['orientdb']['classes'][1]['name']=edge_class_name
    json_struct['loader']['orientdb']['classes'][1]['extends']=edge_superclass 
    json_struct['loader']['orientdb']['indexes'][0]['class']=vertex_class_name
    json_struct['loader']['orientdb']['indexes'][0]['fields']=index_names
    json_struct['loader']['orientdb']['indexes'][0]['type']=type_field
    return json_struct

"""
"transformers": [
    { "merge": { "joinFieldName": "fromId", "lookup": "Person.id" } },
    { "vertex": {"class": "Person", "skipDuplicates": true} },
    { "edge": { "class": "FriendsWith",
                "joinFieldName": "toId",
                "lookup": "Person.id",
                "direction": "out"
              }
    },
    { "field": { "fieldNames": ["fromId", "toId"], "operation": "remove" } }
]
"""

def gen_edge_struct():
    json_struct=dict()
    json_struct['source']={}
    json_struct['source']['file']={}
    json_struct['source']['file']['path']='add-path'
    json_struct['extractor']={}
    json_struct['extractor']['csv']={}
    json_struct['transformers']=[]
    json_struct['transformers'].append({})
    json_struct['transformers'][0]={}
    json_struct['transformers'][0]['merge']={}
    json_struct['transformers'][0]['merge']['joinFieldName']="add-join_name"
    json_struct['transformers'][0]['merge']['lookup']="add_lookup_field"

    print json_struct
    

def reindex_graph(data):
    #print data.columns
    orig_df=data[['nameOrig','nameDest','type']]
    dest_df=data[['nameDest']]
    return orig_df,dest_df
    
  
#paysim=pd.read_csv('paysim.csv')
#paysim_sample=paysim.sample(n=5000,random_state=42)
#paysim_sample.to_csv('paysim_sample.csv')
#paysim=read_data_sample('paysim_sample.csv')
#orig_df,dest_df=reindex_graph(paysim)
#orig_df.to_csv('orig.csv')
#dest_df.to_csv('dest.csv')
gen_edge_struct()