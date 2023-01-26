# Cloud Computing: AWS Project
Fully scalable ETL and Data Analytics  pipeline using AWS (IaC via CloudFormation)

## Problem Overview

Current Set-Up:
- Branch data is isolated 
- Manual upload into standard database
- Time consuming to gather all branch data
- Difficult to identify company wide trends


## Solution Overview

- Fully scalable ETL pipeline:

    - Sanitise data: Remove PII (Personal Identifiable Information)
    - Normalise data to a set standard
    - Monitor infrastructure metrics
    - Data Analytics

- Capable of handling large data volume

- Store data in central data warehouse

- Enable company wide to branch specific data analytics


## Technologies

- ETL pipeline:
    - AWS:
        - CloudFormation
        - S3
        - Lambda
        - SQS(FIFO Queue)
        - Redshift
        - IAM
    - Python
![Control-V](https://user-images.githubusercontent.com/114569343/213544797-68037c66-7360-48a8-9202-a68e45f25cc3.png) 

- Data Analytics (+ Infrastructure Metrics):
    - AWS:
     - Cloudwatch
     - EC2
    - Docker
    - Grafana

![Control-V (1)](https://user-images.githubusercontent.com/114569343/213548528-20fe7772-3417-44c4-86f6-2ec48655a9c7.png)

## Table Schema
![Untitled (1)](https://user-images.githubusercontent.com/114569343/214832903-4a0eb541-5ac8-4848-8cc5-91e8c459c5ea.png)

## Getting Started

-Must haves:
 - AWS account configured with suitable user permissions
 - Redshift cluster (single node) created via console
 - EC2 instance running Docker
 - Grafana installed on Docker

Lambda code must be zipped and uploaded to a deployment bucket (referenced in the cloudformation template)

### Via CLI
```
aws cloudformation deploy --stack-name team4-stack --template-file cloudformation/team4_cloudformation.yaml --region eu-west-1 --capabilities  CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --parameter-overrides ExtractTransformPackageKey=${zipfile} LoadPackageKey=${zipfile}

```
- Include --parameter overrided tag with correct file names 
- ETL code is specific to a certain CSV format: see test data folder

