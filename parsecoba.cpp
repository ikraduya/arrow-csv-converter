#include <cstdint>
#include <iostream>
#include <vector>
#include <fstream>

#include "csv.h"
#include <arrow/api.h>

using arrow::Int64Builder;
using arrow::FloatBuilder;
using arrow::StringBuilder;

arrow::Status csvToColumnarTable(std::string filename, std::shared_ptr<arrow::Table> *table) {
  io::CSVReader<3> in(filename);
  
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  Int64Builder cid_builder(pool);
  StringBuilder name_builder(pool);
  FloatBuilder total_builder(pool);

  in.read_header(io::ignore_extra_column, "cid", "name", "total");

  int cid; std::string name; float total;
  while (in.read_row(cid, name, total)) {
    ARROW_RETURN_NOT_OK(cid_builder.Append(cid));
    ARROW_RETURN_NOT_OK(name_builder.Append(name));
    ARROW_RETURN_NOT_OK(total_builder.Append(total));
  }

  // finalise arrays, declare schema and combine to table
  std::shared_ptr<arrow::Array> cid_array;
  ARROW_RETURN_NOT_OK(cid_builder.Finish(&cid_array));
  std::shared_ptr<arrow::Array> name_array;
  ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
  std::shared_ptr<arrow::Array> total_array;
  ARROW_RETURN_NOT_OK(total_builder.Finish(&total_array));

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
    arrow::field("cid", arrow::int64()),
    arrow::field("name", arrow::utf8()),
    arrow::field("total", arrow::float64())
  };

  auto schema = std::make_shared<arrow::Schema>(schema_vector);

  *table = arrow::Table::Make(schema, {cid_array, name_array, total_array});

  return arrow::Status::OK();
}

arrow::Status columnarTableToCSV(std::shared_ptr<arrow::Table> &table) {
  std::shared_ptr<arrow::Schema> schema = table->schema();
  int num = schema->num_fields();

  std::ofstream outcsv;
  outcsv.open("out.csv");
  
  for (int i=0; i<num; i++) {
    std::shared_ptr<arrow::Field> field = schema->field(i);
    outcsv << field->name();
    if (i != num-1) {
      outcsv << ",";
    }
  }
  outcsv << std::endl;
  
  auto cids = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->data()->chunk(0));
  auto names = std::static_pointer_cast<arrow::StringArray>(table->column(1)->data()->chunk(0));
  auto totals = std::static_pointer_cast<arrow::FloatArray>(table->column(2)->data()->chunk(0));

  for (int64_t i=0; i<table->num_rows(); i++) {
    int64_t cid = cids->Value(i);
    std::string name = names->GetString(i);
    float total = totals->Value(i);
    outcsv << cid << "," << name << "," << total << std::endl;
  }
  outcsv.close();
  
  return arrow::Status::OK();  
}

int main(int argc, char **argv) {
  
  std::shared_ptr<arrow::Table> table;
  csvToColumnarTable("flcust.csv", &table);

  columnarTableToCSV(table);

  assert(table->num_columns() == 3);

  std::cout << "done beib = " << table->num_columns() << std::endl;
  return EXIT_SUCCESS;
}
