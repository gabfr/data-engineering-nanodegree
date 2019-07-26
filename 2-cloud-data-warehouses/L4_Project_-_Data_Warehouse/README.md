# Sparkify Data Warehouse ETL process

## Getting started

Read the sections below to know how to get started:

### Configuration

First of all, you have to copy the `dwh.cfg.example` to a version without the example suffix (`dwh.cfg`), then fill all the fields of configuration. 

Letting only two fields empty: 
 - `HOST` (inside the `DB` configuration section) 
 - And the `ARN` (inside the `IAM_ROLE` configuration section) 

###  Infrastructure provisioning

There are **3 scripts** that will ease our job to create our data warehouse infrastructure:
#### 1. Creating a new AWS Redshift Cluster
```sh
python aws_create_cluster.py
```

#### 2. Checking the cluster availability 

_This one you should run several times until your cluster becomes available - takes from 3 to 6 minutes_

```sh
python aws_check_cluster_available.py
```

#### 3. Destroying the cluster 

_After the ETL process done, nor whenever you want, you can destroy it with a single command:_

```sh
python aws_destroy_cluster.py
```