#include <iostream>
#include <fstream>
#include <vector>
#include <iterator>
#include <algorithm>
#include <boost/algorithm/string.hpp>

/*
 * Class to read data from a csv file
 */
class CSVReader {
  private:
    std::string filename;
    std::string delimeter;
  
  public:
    CSVReader(std::string filename, std::string delm=",") :
      filename(filename), delimeter(delm) 
    { }

    // function to fetch data from a CSV file
    std::vector<std::vector<std::string>> getData() {
      std::ifstream file(filename);
      std::vector<std::vector<std::string>> dataList;
      
      std::string line = "";
      getline(file, line);  // dump the first row (header)
      while (getline(file, line)) {
        std::vector<std::string> vec;
        boost::algorithm::split(vec, line, boost::is_any_of(delimeter));
        dataList.push_back(vec);
      }
      file.close();

      return dataList;
    }

    // function to get csv header
    std::vector<std::string> getHeader() {
      std::ifstream file(filename);
      std::vector<std::string> header;

      std::string line = "";  
      getline(file, line);  // onlu get the first row
      boost::algorithm::split(header, line, boost::is_any_of(delimeter));
      file.close();

      return header;
    }
};