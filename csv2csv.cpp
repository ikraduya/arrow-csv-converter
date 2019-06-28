#include <cstdint>
#include <iostream>
#include <vector>
#include <tuple>
#include <fstream>
#include <map>
#include <memory>

#include "CSVReader.hpp"
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/dataset/api.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

using arrow::Int32Builder;
using arrow::StringBuilder;
using arrow::FloatBuilder;
using arrow::DoubleBuilder;
using arrow::BooleanBuilder;

typedef std::tuple<std::string,std::shared_ptr<arrow::DataType>> data_type_tup_t;

// prepare boolean map
std::map<std::string, bool> str_bool_map = {
  {"true", true}, {"false", false},
  {"True", true}, {"False", false},
  {"TRUE", true}, {"FALSE", false},
  {"T", true}, {"F", false},
  {"1", true}, {"0", false}
};
std::map<bool, std::string> bool_str_map = {
  {true, "true"}, {false, "false"}
};

arrow::Status csvToColumnarTable(
  std::vector<std::vector<std::string>> csvData,
  std::vector<data_type_tup_t> dataTypeVec,
  std::shared_ptr<arrow::Table> *table) {
  
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  // make schema
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  // determine the builders
  std::vector<arrow::ArrayBuilder*> builders;
  for (data_type_tup_t type : dataTypeVec) {
    std::string first = std::get<0>(type);
    std::shared_ptr<arrow::DataType> second = std::get<1>(type);
    boost::trim(first);
    
    if (second->Equals(arrow::int32())) {
      schema_vector.push_back(arrow::field(first, arrow::int32()));
      builders.push_back(new Int32Builder(pool));
    } else if (second->Equals(arrow::float32())) {
      builders.push_back(new FloatBuilder(pool));
      schema_vector.push_back(arrow::field(first, arrow::float32()));
    } else if (second->Equals(arrow::float64())) {
      builders.push_back(new DoubleBuilder(pool));
      schema_vector.push_back(arrow::field(first, arrow::float64()));
    } else if (second->Equals(arrow::utf8())) {
      builders.push_back(new StringBuilder(pool));
      schema_vector.push_back(arrow::field(first, arrow::utf8()));
    } else if (second->Equals(arrow::boolean())) {
      builders.push_back(new BooleanBuilder(pool));
      schema_vector.push_back(arrow::field(first, arrow::boolean()));
    }
  }

  for (std::vector<std::string> row : csvData) {
    int builderLen = builders.size();
    for (int i=0; i<builderLen; i++) {
      std::shared_ptr<arrow::DataType> dataType = std::get<1>(dataTypeVec.at(i));
      std::string col = row.at(i);
      boost::trim(col);
      
      if (dataType->Equals(arrow::int32())) {
        dynamic_cast<Int32Builder*>( builders.at(i) )->Append(boost::lexical_cast<int>(col)); 
      } else if (dataType->Equals(arrow::float32())) {
        dynamic_cast<FloatBuilder*>( builders.at(i) )->Append(boost::lexical_cast<float>(col)); 
      } else if (dataType->Equals(arrow::float64())) {
        dynamic_cast<DoubleBuilder*>( builders.at(i) )->Append(boost::lexical_cast<double>(col)); 
      } else if (dataType->Equals(arrow::utf8())) {
        dynamic_cast<StringBuilder*>( builders.at(i) )->Append(col); 
      } else if (dataType->Equals(arrow::boolean())) {
        dynamic_cast<BooleanBuilder*>( builders.at(i) )->Append(str_bool_map.find(col)->second); 
      }
    }
  }

  // finalise arrays, declare schema and combine to arrays
  arrow::ArrayVector arrVector;
  std::vector<std::shared_ptr<arrow::Array>> array_array;
  for (arrow::ArrayBuilder* builder : builders) {
    std::shared_ptr<arrow::Array> arr;
    array_array.push_back(arr);
    builder->Finish(&arr);
    arrVector.push_back(arr);
  }

  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  *table = arrow::Table::Make(schema, arrVector);

  return arrow::Status::OK();
}

arrow::Status columnarTableToCSV(std::shared_ptr<arrow::Table> &table, std::string filename) {
  std::shared_ptr<arrow::Schema> schema = table->schema();
  int num_field = schema->num_fields();

  std::ofstream outcsv;
  outcsv.open(filename);
  
  // write headers and get data cols
  std::vector<std::shared_ptr<arrow::Array>> cols_vec;
  std::vector<int> chunk_tracker;
  for (int i=0; i<num_field; i++) {
    std::shared_ptr<arrow::Field> field = schema->field(i);
    outcsv << field->name();
    if (i != num_field-1) {
      outcsv << ",";
    }

    if (field->type()->Equals(arrow::int32())) {
      cols_vec.push_back(std::static_pointer_cast<arrow::Int32Array>(table->column(i)->data()->chunk(0)));
    } else if (field->type()->Equals(arrow::float32())) {
      cols_vec.push_back(std::static_pointer_cast<arrow::FloatArray>(table->column(i)->data()->chunk(0)));
    } else if (field->type()->Equals(arrow::float64())) {
      cols_vec.push_back(std::static_pointer_cast<arrow::DoubleArray>(table->column(i)->data()->chunk(0)));
    } else if (field->type()->Equals(arrow::utf8())) {
      cols_vec.push_back(std::static_pointer_cast<arrow::StringArray>(table->column(i)->data()->chunk(0)));
    } else if (field->type()->Equals(arrow::boolean())) {
      cols_vec.push_back(std::static_pointer_cast<arrow::BooleanArray>(table->column(i)->data()->chunk(0)));
    }
    chunk_tracker.push_back(0);
  }
  outcsv << std::endl;

  int64_t num_row = table->num_rows(), num_col = table->num_columns();
  for (int64_t i=0; i<num_row; i++) {
    for (int64_t j=0; j<num_col; j++) {
      std::shared_ptr<arrow::Array> col_arr = cols_vec.at(j);
      std::shared_ptr<arrow::DataType> type = col_arr->type();
      if (type->Equals(arrow::int32())) {
        outcsv << dynamic_cast<arrow::Int32Array*>( &(*col_arr) )->Value(i);
      } else if (type->Equals(arrow::float32())) {
        outcsv << dynamic_cast<arrow::FloatArray*>( &(*col_arr) )->Value(i);
      } else if (type->Equals(arrow::float64())) {
        outcsv << dynamic_cast<arrow::DoubleArray*>( &(*col_arr) )->Value(i);
      } else if (type->Equals(arrow::utf8())) {
        outcsv << dynamic_cast<arrow::StringArray*>( &(*col_arr) )->GetString(i);
      } else if (type->Equals(arrow::boolean())) {
        outcsv << bool_str_map.find(dynamic_cast<arrow::BooleanArray*>( &(*col_arr) )->Value(i))->second;
      }
      if (j < num_col-1) {
        outcsv << ",";
      } else {
        outcsv << std::endl;
      }
    }
  }
  outcsv.close();
  
  return arrow::Status::OK();  
}

void printOutTable(const std::shared_ptr<arrow::Table> &table) {
  int num_col = table->num_columns();
  int num_row = table->num_rows();

  // print cols name
  std::vector<std::string> cols_name;
  for (int i=0; i<num_col; i++) {
    std::string col_name = table->column(i)->name();
    std::cout << col_name << std::endl;
    
    std::shared_ptr<arrow::ChunkedArray> col_chunked_array = table->GetColumnByName(col_name)->data();
    int num_chunk = col_chunked_array->num_chunks();
    std::cout << "Num chunk: " << num_chunk << std::endl;
    for (int j=0; j<num_chunk; j++) {
      std::cout << col_chunked_array->chunk(j)->ToString() << std::endl;
    }
    std::cout << std::endl;
  }

  std::cout << "done" << std::endl;
}

/*
 * Data Types file format:
 *  <type>, <type>, <type>
 * 
 * supported data type:
 *  - integer
 *  - float
 *  - double
 *  - string
 *  - boolean
 */
std::vector<data_type_tup_t> parseDataTypeString(std::string dataTypes, std::vector<std::string> header) {
  std::vector<std::string> vec;
  boost::algorithm::split(vec, dataTypes, boost::is_any_of(","));

  assert(vec.size() == header.size());
  std::vector<data_type_tup_t> dataTypeVec;
  int headerLen = header.size();
  for (int i=0; i<headerLen; i++) {
    std::string type = vec.at(i);
    std::string col_name = header.at(i);
    if (type.compare("integer") == 0) {
      dataTypeVec.push_back(std::make_tuple(col_name, arrow::int32()));
    } else if (type.compare("float") == 0) {
      dataTypeVec.push_back(std::make_tuple(col_name, arrow::float32()));
    } else if (type.compare("double") == 0) {
      dataTypeVec.push_back(std::make_tuple(col_name, arrow::float64()));
    } else if (type.compare("string") == 0) {
      dataTypeVec.push_back(std::make_tuple(col_name, arrow::utf8()));
    } else if (type.compare("boolean") == 0) {
      dataTypeVec.push_back(std::make_tuple(col_name, arrow::boolean()));
    }
  }

  return dataTypeVec;
}

int main(int argc, char **argv) {
  // validating usage
  if (argc < 4) {
    std::cout << "Usage: ./csv2csv <input> <dataTypes> <output>" << std::endl;
    return EXIT_FAILURE;
  }
  std::string fin = argv[1], dataTypes = argv[2], fout = argv[3];

  // import from csv
  CSVReader reader(fin);
  std::vector<std::string> csvHeader = reader.getHeader();
  std::vector<std::vector<std::string>> csvData = reader.getData();

  // convert to arrow
  std::vector<data_type_tup_t> dataTypeVec = parseDataTypeString(dataTypes, csvHeader);
  std::shared_ptr<arrow::Table> table;
  csvToColumnarTable(csvData, dataTypeVec, &table);

  // write out data
  assert(table->num_columns() == csvHeader.size());
  printOutTable(table);

  // export to csv
  columnarTableToCSV(table, fout);

  return EXIT_SUCCESS;
}
