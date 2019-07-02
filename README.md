# Requirement
## C++ Arrow Library
install c++ arrow library: https://arrow.apache.org/install/

# Usage
## csv2csv
### Compile
g++ csv2csv.cpp -o csv2csv -larrow -lpthread

### Run
./csv2csv FL_insurance_sample.csv integer,string,string,float,float,float,float,float,float,float,float,float,float,double,double,string,string,integer fl_out 4

## csv2parquet
### Compile
g++ csv2parquet.cpp -o csv2parquet -larrow -lparquet -lpthread

### Run
./csv2parquet FL_insurance_sample.csv integer,string,string,float,float,float,float,float,float,float,float,float,float,double,double,string,string,integer fl_out 4

## csv2feather
### Compile
g++ csv2feather.cpp -o csv2feather -larrow -lpthread

### Run
./csv2feather FL_insurance_sample.csv integer,string,string,float,float,float,float,float,float,float,float,float,float,double,double,string,string,integer fl_out 4