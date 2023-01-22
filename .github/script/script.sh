#!/bin/bash 
  
echo 'Starting to Deploy...'
version=`date +%Y%m%d%H%M%S`
zipfile = etl_v${version}.zip

zip lambda_function.zip lambda_function.py
zip -r ${zipfile} aws lambda_function.zip src/
aws s3 rm s3://team4-deployment/lambda_function.zip
aws s3 rm s3://team4-deployment/src.zip
aws cp ${zipfile} s3://team4-deployment

aws cloudformation deploy --stack-name team4-stack --template-file cloudformation/team4_cloudformation.yaml --region eu-west-1 --capabilities  CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --parameter-overrides ExtractTransformPackageKey=${zipfile} LoadPackageKey=${zipfile}
 