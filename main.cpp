#include <iostream>
#include <map>
#include <stdexcept>
#include <vector>

#define MYSQLPP_MYSQL_HEADERS_BURIED
#include <mysql++/mysql++.h>
using mysqlpp::Connection, mysqlpp::Query, mysqlpp::Row, mysqlpp::UseQueryResult;

using PrimaryKey = std::vector<std::string>;

// using outputter_t = void(Query& query, const Row&, const std::vector<std::string>& field_names, int index);


struct TableData {
	const std::string full_table_name;
	std::map<PrimaryKey, Row> rows;

	explicit TableData(std::string full_table_name) : full_table_name(std::move(full_table_name)) { }
};

class TableMetadata {
public:
	const int field_count;

private:
	std::vector<std::string> field_names;
	std::list<int> all_indexes;
	std::list<int> primary_key_indexes;

	typedef void (TableMetadata::*outputter_t)(Query& query, const Row&, int index) const;

	void output_field(Query& query, const Row&, int index) const {
		query << "`" << field_names[index] << "`";
	}

	void output_value(Query& query, const Row& row, int index) const {
		query << mysqlpp::quote << row[index];
	}

	void output_equal(Query& query, const Row& row, int index) const {
		output_field(query, row, index);
		query << '=';
		output_value(query, row, index);
	}

	template<class LIST>
	bool output_list(Query& query, const Row& row, outputter_t outputter, const char* delimiter, const LIST& indexes) const {
		bool writing_started = false;
		for (int index : indexes) {
			if (writing_started) {
				query << delimiter;
			}
			(this->*outputter)(query, row, index);
			writing_started = true;
		}
		return writing_started;
	}

public:
	TableMetadata(std::vector<std::string> field_names, std::list<int> primary_key_indexes)
	: field_count(static_cast<int>(field_names.size())), field_names(std::move(field_names)), primary_key_indexes(std::move(primary_key_indexes)) {
		if (this->field_names.size() > std::numeric_limits<int>::max()) {
			throw std::runtime_error("strangely too many columns in database");
		}
		for (int i=0; i<field_count; ++i) {
			all_indexes.push_back(i);
		}
	}

	bool operator!=(const TableMetadata& that) const {
		return field_names != that.field_names || primary_key_indexes != that.primary_key_indexes;
	}

	template<class LIST>
	bool output_equal_list_for_update(Query& query, const Row& row, const LIST& indexes) const {
		return output_list(query, row, &TableMetadata::output_equal, ",", indexes);
	}

	bool output_equal_list_for_where(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_equal, " AND ", primary_key_indexes);
	}

	bool output_field_list_for_insert(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_field, ",", all_indexes);
	}

	bool output_value_list_for_insert(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_value, ",", all_indexes);
	}

	[[nodiscard]] PrimaryKey extract_keys(const Row& row) const {
		PrimaryKey keys;
		for (int index : primary_key_indexes) {
			keys.emplace_back(row[index]);
		}
		return keys;
	}
};

TableMetadata extract_table_metadata(Connection& conn, const std::string& full_table_name) {
	std::vector<std::string> field_names;
	std::list<int> primary_key_indexes;
	if (Query query = conn.query("DESCRIBE " + full_table_name)) {
		if (UseQueryResult res = query.use()) {
			int index = 0;
			while (Row row = res.fetch_row()) {
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

TableData fetch_table_data(Connection& conn, const TableMetadata& metadata, const std::string& full_table_name) {
	TableData table_data(full_table_name);
	if (Query query = conn.query("SELECT * FROM " + full_table_name)) {
		if (UseQueryResult res = query.use()) {
			while (Row row = res.fetch_row()) {
				PrimaryKey keys = metadata.extract_keys(row);
				table_data.rows.emplace(std::move(keys), std::move(row));
			}
		}
	}
	return table_data;
}

void compute_table_diff(Connection& conn, const TableMetadata& metadata, const std::string& full_table_name, TableData& table_data) {
	if (Query query = conn.query("SELECT * FROM " + full_table_name)) {
		if (UseQueryResult res = query.use()) {
			std::vector<int> changed_indexes;
			while (Row row = res.fetch_row()) {
				PrimaryKey keys = metadata.extract_keys(row);

				auto it = table_data.rows.find(keys);
				if (it == table_data.rows.end()) {
					// if the row is not present in table_data, it should be INSERTed
					Query insert_query = conn.query();
					insert_query << "INSERT INTO " + table_data.full_table_name + " (";
					metadata.output_field_list_for_insert(insert_query, row);
					insert_query << ") VALUES (";
					metadata.output_value_list_for_insert(insert_query, row);
					insert_query << ")";

					std::cout << insert_query << ";\n";
				} else {
					// it is present, but it may have changed
					changed_indexes.clear();
					for (int index=0; index<metadata.field_count; ++index) {
						if (row[index] != it->second[index]) {
							changed_indexes.push_back(index);
						}
					}
					if (!changed_indexes.empty()) {
						Query update_query = conn.query();
						update_query << "UPDATE " + table_data.full_table_name + " SET ";
						metadata.output_equal_list_for_update(update_query, row, changed_indexes);
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
		Query delete_query = conn.query();
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
		Connection conn1(nullptr, server1, user1, password1);
		Connection conn2(nullptr, server2, user2, password2);

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
