#define MYSQLPP_MYSQL_HEADERS_BURIED
#include <mysql++/mysql++.h>
#include <iostream>
#include <map>
#include <stdexcept>
#include <vector>

using Composite = std::vector<std::string>;

template<class VALUES>
using outputter_t = void(mysqlpp::Query& stream, const VALUES&, const std::vector<std::string>& field_names, int index);

template<class VALUES>
void output_field(mysqlpp::Query& stream, const VALUES&, const std::vector<std::string>& field_names, int index) {
	stream << "`" << field_names[index] << "`";
}

template<class VALUES>
void output_value(mysqlpp::Query& stream, const VALUES& values, const std::vector<std::string>&, int index) {
	stream << mysqlpp::quote << values[index];
}

template<class VALUES>
void output_equal(mysqlpp::Query& stream, const VALUES& values, const std::vector<std::string>& field_names, int index) {
	output_field(stream, values, field_names, index);
	stream << '=';
	output_value(stream, values, field_names, index);
}

struct TableData {
	const std::string full_table_name;
	std::map<Composite, mysqlpp::Row> rows;

	explicit TableData(std::string full_table_name) : full_table_name(std::move(full_table_name)) { }
};

class TableMetadata {
	std::vector<std::string> field_names;
	std::list<int> all_indexes;
	std::list<int> primary_key_indexes;

	template<class VALUES>
	bool output_list(mysqlpp::Query& stream, const VALUES& values, outputter_t<VALUES> outputter, const char* delimiter, const std::list<int>* indexes) const {
		bool writing_started = false;
		for (int index : *indexes) {
			if (writing_started) {
				stream << delimiter;
			}
			outputter(stream, values, field_names, index);
			writing_started = true;
		}
		return writing_started;
	}

public:
	TableMetadata(std::vector<std::string> field_names, std::list<int> primary_key_indexes)
	: field_names(std::move(field_names)), primary_key_indexes(std::move(primary_key_indexes)) {
		for (int i=0; i<this->field_names.size(); ++i) {
			all_indexes.push_back(i);
		}
	}

	bool operator!=(const TableMetadata& that) const {
		return field_names != that.field_names || primary_key_indexes != that.primary_key_indexes;
	}

	template<class VALUES>
	bool output_equal_list_for_where(mysqlpp::Query& stream, const VALUES& values) const {
		return output_list(stream, values, output_equal<VALUES>, " AND ", &primary_key_indexes);
	}

	template<class VALUES>
	bool output_equal_list_for_update(mysqlpp::Query& stream, const VALUES& values, const std::list<int>* indexes) const {
		return output_list(stream, values, output_equal<VALUES>, ",", indexes);
	}

	template<class VALUES>
	bool output_field_list_for_insert(mysqlpp::Query& stream, const VALUES& values) const {
		return output_list(stream, values, output_field<VALUES>, ",", &all_indexes);
	}

	template<class VALUES>
	bool output_value_list_for_insert(mysqlpp::Query& stream, const VALUES& values) const {
		return output_list(stream, values, output_value<VALUES>, ",", &all_indexes);
	}

	[[nodiscard]] Composite extract_keys(const mysqlpp::Row& row) const {
		Composite keys;
		for (int index : primary_key_indexes) {
			keys.emplace_back(row[index]);
		}
		return keys;
	}
};

TableMetadata extract_table_metadata(mysqlpp::Connection& conn, const std::string& full_table_name) {
	std::vector<std::string> field_names;
	std::list<int> primary_key_indexes;
	if (mysqlpp::Query query = conn.query("DESCRIBE " + full_table_name)) {
		if (mysqlpp::UseQueryResult res = query.use()) {
			int index = 0;
			while (mysqlpp::Row row = res.fetch_row()) {
				field_names.emplace_back(row["Field"]);
				if (row["Key"] == "PRI") {
					primary_key_indexes.push_back(index);
				}
				++index;
			}
		}
	}
	return {std::move(field_names), std::move(primary_key_indexes)};
}

TableData fetch_table_data(mysqlpp::Connection& conn, const TableMetadata& metadata, const std::string& full_table_name) {
	TableData table_data(full_table_name);
	if (mysqlpp::Query query = conn.query("SELECT * FROM " + full_table_name)) {
		if (mysqlpp::UseQueryResult res = query.use()) {
			while (mysqlpp::Row row = res.fetch_row()) {
				Composite keys = metadata.extract_keys(row);
				table_data.rows.emplace(std::move(keys), std::move(row));
			}
		}
	}
	return table_data;
}

void compute_table_diff(mysqlpp::Connection& conn, const TableMetadata& metadata, const std::string& full_table_name, TableData& table_data) {
	if (mysqlpp::Query query = conn.query("SELECT * FROM " + full_table_name)) {
		if (mysqlpp::UseQueryResult res = query.use()) {
			std::list<int> changed_indexes;
			while (mysqlpp::Row row = res.fetch_row()) {
				Composite keys = metadata.extract_keys(row);

				auto it = table_data.rows.find(keys);
				if (it == table_data.rows.end()) {
					// if the row is not present in table_data, it should be INSERTed
					mysqlpp::Query insert_query = conn.query();
					insert_query << "INSERT INTO " + table_data.full_table_name + " (";
					metadata.output_field_list_for_insert(insert_query, row);
					insert_query << ") VALUES (";
					metadata.output_value_list_for_insert(insert_query, row);
					insert_query << ")";

					std::cout << insert_query << ";\n";
				} else {
					// it is present, but it may have changed
					changed_indexes.clear();
					const int row_size = row.size();
					for (int index=0; index<row_size; ++index) {
						if (row[index] != it->second[index]) {
							changed_indexes.push_back(index);
						}
					}
					if (!changed_indexes.empty()) {
						mysqlpp::Query update_query = conn.query();
						update_query << "UPDATE " + table_data.full_table_name + " SET ";
						metadata.output_equal_list_for_update(update_query, row, &changed_indexes);
						update_query << " WHERE ";
						metadata.output_equal_list_for_where(update_query, row);

						std::cout << update_query << ";\n";
					}
					table_data.rows.erase(it);
				}
			}
		}
	}

	// afterwards, all rows that are left in table_data are the ones that should be DELETEd
	for (const auto& old : table_data.rows) {
		mysqlpp::Query delete_query = conn.query();
		delete_query << "DELETE FROM " + table_data.full_table_name + " WHERE ";
		metadata.output_equal_list_for_where(delete_query, old.second);

		std::cout << delete_query << ";\n";
	}
}


int main() {
	const char* server1 = "localhost";
	const char* user1 = "root";
	const char* password1 = "qwerty";
	const std::string table_name_1 = "airport_directory.common_airport_2";

	const char* server2 = "localhost";
	const char* user2 = "root";
	const char* password2 = "qwerty";
	const std::string table_name_2 = "airport_directory.common_airport";

	try {
		mysqlpp::Connection conn1(nullptr, server1, user1, password1);
		mysqlpp::Connection conn2(nullptr, server2, user2, password2);

		TableMetadata metadata = extract_table_metadata(conn1, table_name_1);
		if (extract_table_metadata(conn2, table_name_2) != metadata) {
			throw std::runtime_error("table definitions differ");
		}

		TableData data1 = fetch_table_data(conn1, metadata, table_name_1);
		compute_table_diff(conn2, metadata, table_name_2, data1);

	} catch (const std::exception& e) {
		std::cerr << "ERROR!" << e.what() << std::endl;
	}

	return 0;
}
