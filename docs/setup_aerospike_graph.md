 **Starting Aerospike Graph Service and Aerospike with Aerolab****

Aerolab can be downloaded from https://github.com/aerospike/aerolab.

## GCP

### Example of setting up Aerospike in GCP

```
aerolab cluster create -n demo -c 2 --instance n2-standard-8 --zone us-west4-a --disk=pd-ssd:300 --disk=local-ssd@1 --disk=pd-ssd:380@1 --gcp-expire 200h
```

### Example of setting up Aerospike Graph Service in GCP

```
aerolab client create graph -n graph --count 1 --cluster-name demo --namespace test --zone us-west4-a --instance c2-standard-8 --gcp-expire 200h
```

## AWS

### Example of setting up Aerospike in AWS

```
aerolab cluster create -n demo -c 3 -I i4i.2xlarge --aws-expire 120h
```

### Example of setting up Aerospike Graph Service in AWS

```
aerolab client create v graph -n graph --count 1 --cluster-name aim-cluster --namespace test -I i4i.2xlarge
```

## Destroying a cluster

```
aerolab cluster destroy -n <name>
```

## Common tasks after setup

### SSH into a node

```
ssh -i aerolab-graph_us-west-1
```

### Verifying Aerospike Graph Service is running

```
sudo docker ps -a
```

### Launch Gremlin Console using Aerolab

```
aerolab client attach -n graph -- docker run -it --rm tinkerpop/gremlin-console
```

### Access Terminal on Aerospike Graph Service

```
aerolab attach client -n graph
```
**
