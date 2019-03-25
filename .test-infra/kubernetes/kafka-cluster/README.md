## Kafka test cluster

The kubernetes config files in this directory create a Kafka cluster
comprised of 3 Kafka replicas and 3 Zookeeper replicas. They
expose themselves using 3 LoadBalancer services. To deploy the cluster, simply run

    sh setup-cluster.sh
    
Before executing the script, ensure that your account has cluster-admin 
privileges, as setting RBAC cluster roles requires that.

The scripts are based on [Yolean kubernetes-kafka](https://github.com/Yolean/kubernetes-kafka)

Scripts were tested on GKE.