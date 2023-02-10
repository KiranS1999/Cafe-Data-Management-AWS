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

![ETL pipeline](https://user-images.githubusercontent.com/114569343/218149477-b014fe41-3819-41a3-859a-61ff083a6048.PNG)

- Data Analytics (+ Infrastructure Metrics):
    - AWS:
     - Cloudwatch
     - EC2
    - Docker
    - Grafana

![Grafana](https://user-images.githubusercontent.com/114569343/218149462-99298b32-cde1-45b8-82a1-1df1c98bf015.PNG)

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

