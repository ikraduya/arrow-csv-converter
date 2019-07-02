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
      std::vector<std::string> header_vec;
      boost::algorithm::split(header_vec, line, boost::is_any_of(delimeter));
      int num_col = header_vec.size();
      
      while (getline(file, line)) {
        std::vector<std::string> vec;
        boost::algorithm::split(vec, line, boost::is_any_of(delimeter));
        int vec_len = vec.size();
        if (vec_len == 1) {   // corner case: newline on last column
          dataList.back().back().append("\n");
          dataList.back().back().append(vec.back());
          continue;
        }
        while (vec_len < num_col) { // new line in one col
          getline(file, line);
          std::vector<std::string> vec_extra;
          boost::algorithm::split(vec_extra, line, boost::is_any_of(delimeter));

          vec.back().append("\n");
          vec.back().append(vec_extra.at(0));

          vec_extra.erase( vec_extra.begin() );
          vec.insert(vec.end(), vec_extra.begin(), vec_extra.end());
          vec_len += vec_extra.size();
        }

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
      getline(file, line);  // only get the first row
      boost::algorithm::split(header, line, boost::is_any_of(delimeter));
      std::cout << "header: " << line << std::endl;

      file.close();

      return header;
    }
};