# Spark Dependency Discovery
Functional depencency discovery of big data using spark. Uses a priori-type algorithm to efficiently discover which depencencies to investigate on a lattice, combined with iterative sampling. This is a project for the TU/e course 2AMD15 Big Data Management.


Dataset: https://s3.amazonaws.com/dl.ncsbe.gov/data/Snapshots/VR_Snapshot_20051125.zip

From: https://dl.ncsbe.gov/index.html?prefix=data/Snapshots

## Pre-processing
Pre-processed dataset and layout at: https://drive.google.com/drive/folders/1CnlWr0MkRa_nh3yTn5_kuEyJSRcL66Q5?usp=sharing

To run the script:
- Set input file path and output file folder
- Either run the script from your IDE
- Or via terminal by running the following command from your spark installation folder:
```bin\spark-submit path_to_script\Preprocesser.py```
