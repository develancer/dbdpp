#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <stdexcept>
#include <vector>

#include <mysql++/mysql++.h>
using mysqlpp::Connection, mysqlpp::Query, mysqlpp::Row, mysqlpp::UseQueryResult;

struct Config {
	std::string host;
	std::string user;
	std::string password;
	std::string database;
};

class ConfigParser {
	std::string path;

	static std::string unescape_string(const std::string& str, char end_of_line) {
		std::string result;
		bool escape = false;

		for (char c : str) {
			if (escape) {
				switch (c) {
				case 'b': result += '\b';
					break;
				case 't': result += '\t';
					break;
				case 'n': result += '\n';
					break;
				case 'r': result += '\r';
					break;
				case '\\': result += '\\';
					break;
				case 's': result += ' ';
					break;
				default: if (c != end_of_line) result += '\\';
					result += c;
					break;
				}
				escape = false;
			}
			else if (c == '\\') {
				escape = true;
			}
			else {
				if (c == end_of_line) {
					break;
				}
				result += c;
			}
		}

		if (escape) {
			result += '\\';
		}

		return result;
	}

	static void trim_whitespace(std::string& str) {
		str.erase(0, str.find_first_not_of(" \t\n\r\f\v"));
		auto index = str.find_last_not_of(" \t\n\r\f\v");
		if (index != std::string::npos) {
			str.erase(index + 1);
		}
	}

	std::map<std::string, std::string> parse_config_entries(std::ifstream&& file) const {
		std::map<std::string, std::string> config;
		if (!file.is_open()) {
			throw std::runtime_error("cannot open config file " + path);
		}

		std::string line;
		while (std::getline(file, line)) {
			trim_whitespace(line);

			// Ignore comments and sections
			if (line.empty() || line[0] == '#' || line[0] == ';' || line[0] == '[') {
				continue;
			}

			// Find the position of the equals sign
			size_t pos = line.find('=');
			if (pos != std::string::npos) {
				std::string key = line.substr(0, pos);
				if (key.find('#') != std::string::npos) {
					continue; // = sign is a part of comment, not an actual entry
				}
				std::string value = line.substr(pos + 1);

				// Trim leading and trailing whitespace from key and value
				trim_whitespace(key);
				trim_whitespace(value);

				// Remove quotes from value if present
				char end_of_line = '#';
				if (!value.empty() && (value.front() == '\'' || value.front() == '"')) {
					end_of_line = value.front();
					value = value.substr(1);
				}
				// Unescape the value
				value = unescape_string(value, end_of_line);

				// Insert the key-value pair into the map
				config[key] = value;
			}
		}

		return config;
	}

	[[nodiscard]] std::string get_entry(const std::map<std::string, std::string>& entries, const std::string& key) const {
		auto it = entries.find(key);
		if (it == entries.end()) {
			throw std::runtime_error("missing " + key + " in config file " + path);
		}
		return it->second;
	}

public:
	explicit ConfigParser(std::string path) : path(std::move(path)) { }

	[[nodiscard]] Config parse_config() const {
		auto entries = parse_config_entries(std::ifstream(path));

		Config config;
		config.host = get_entry(entries, "host");
		config.password = get_entry(entries, "password");
		config.user = get_entry(entries, "user");
		config.database = entries["database"];

		auto it = entries.find("port");
		if (it != entries.end()) {
			config.host += ':' + it->second;
		}

		return config;
	}
};

using PrimaryKey = std::vector<std::string>;

struct TableData {
	const std::string full_table_name;
	std::map<PrimaryKey, Row> rows;

	explicit TableData(std::string full_table_name) : full_table_name(std::move(full_table_name)) {
	}
};

class TableMetadata {
public:
	const int field_count;

private:
	std::vector<std::string> field_names;
	std::list<int> all_indexes;
	std::list<int> primary_key_indexes;
	std::list<int> non_primary_key_indexes;

	typedef void (TableMetadata::*outputter_t)(Query& query, const Row&, int index) const;

	void output_field(Query& query, const Row&, int index) const {
		query << "`" << field_names[index] << "`";
	}

	void output_value(Query& query, const Row& row, int index) const {
		query << mysqlpp::quote << row[index];
	}

	void output_null_field(Query& query, const Row& row, int index) const {
		query << "j.";
		output_field(query, row, index);
		query << " IS NULL";
	}

	void output_equal(Query& query, const Row& row, int index) const {
		output_field(query, row, index);
		query << '=';
		output_value(query, row, index);
	}

	void output_diff(Query& query, const Row& row, int index) const {
		query << "(NOT BINARY s.";
		output_field(query, row, index);
		query << " <=> t.";
		output_field(query, row, index);
		query << ")";
	}

	template <class LIST>
	bool output_list(Query& query, const Row& row, outputter_t outputter, const char* delimiter,
	                 const LIST& indexes) const {
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
		: field_count(static_cast<int>(field_names.size())), field_names(std::move(field_names)),
		  primary_key_indexes(std::move(primary_key_indexes)) {
		if (this->field_names.size() > std::numeric_limits<int>::max()) {
			throw std::runtime_error("strangely too many columns in database");
		}
		for (int i = 0; i < field_count; ++i) {
			all_indexes.push_back(i);
		}
		std::set_difference(
			all_indexes.begin(), all_indexes.end(),
			this->primary_key_indexes.begin(), this->primary_key_indexes.end(),
			std::inserter(non_primary_key_indexes, non_primary_key_indexes.end())
		);
	}

	bool operator!=(const TableMetadata& that) const {
		return field_names != that.field_names || primary_key_indexes != that.primary_key_indexes;
	}

	template <class LIST>
	bool output_equal_list_for_update(Query& query, const Row& row, const LIST& indexes) const {
		return output_list(query, row, &TableMetadata::output_equal, ",", indexes);
	}

	bool output_equal_list_for_where(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_equal, " AND ", primary_key_indexes);
	}

	bool output_null_key_list_for_where(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_null_field, " AND ", primary_key_indexes);
	}

	bool output_diff_list_for_where(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_diff, " OR ", non_primary_key_indexes);
	}

	bool output_key_list_for_using(Query& query, const Row& row) const {
		return output_list(query, row, &TableMetadata::output_field, ",", primary_key_indexes);
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
			std::string key;
			row[index].to_string(key);
			keys.emplace_back(std::move(key));
		}
		return keys;
	}
};

template<class VISITOR>
void process_rows_from_query(Connection& conn, Query& query, VISITOR visitor) {
	if (UseQueryResult res = query.use()) {
		while (Row row = res.fetch_row()) {
			visitor(row);
		}
	}
}

template<class VISITOR>
void process_rows_from_query(Connection& conn, const std::string& sql, VISITOR visitor) {
	if (Query query = conn.query(sql)) {
		process_rows_from_query<VISITOR>(conn, query, visitor);
	}
}

TableMetadata extract_table_metadata(Connection& conn, const std::string& full_table_name) {
	std::vector<std::string> field_names;
	std::list<int> primary_key_indexes;
	int index = 0;
	process_rows_from_query(conn, "DESCRIBE " + full_table_name, [&](const Row& row) {
		field_names.emplace_back(row["Field"]);
		if (row["Key"] == "PRI") {
			primary_key_indexes.push_back(index);
		}
		++index;
	});
	return {std::move(field_names), std::move(primary_key_indexes)};
}

TableData fetch_table_data(Connection& conn, const TableMetadata& metadata, const std::string& full_table_name) {
	TableData table_data(full_table_name);
	process_rows_from_query(conn, "SELECT * FROM " + full_table_name, [&](Row& row) {
		PrimaryKey keys = metadata.extract_keys(row);
		table_data.rows.emplace(std::move(keys), std::move(row));
	});
	return table_data;
}

void print_delete(Connection& conn, const TableMetadata& metadata, const Row& row, const std::string& target_table_name) {
	Query delete_query = conn.query();
	delete_query << "DELETE FROM " + target_table_name + " WHERE ";
	if (!metadata.output_equal_list_for_where(delete_query, row)) {
		return;
	}

	std::cout << delete_query << ";\n";
}

void print_insert(Connection& conn, const TableMetadata& metadata, const Row& row, const std::string& target_table_name) {
	Query insert_query = conn.query();
	insert_query << "INSERT INTO " + target_table_name + " (";
	if (!metadata.output_field_list_for_insert(insert_query, row)) {
		return;
	}
	insert_query << ") VALUES (";
	if (!metadata.output_value_list_for_insert(insert_query, row)) {
		return;
	}
	insert_query << ")";

	std::cout << insert_query << ";\n";
}

void print_update(Connection& conn, const TableMetadata& metadata, const Row& row, const std::string& target_table_name, const std::vector<int>& changed_indexes) {
	Query update_query = conn.query();
	update_query << "UPDATE " + target_table_name + " SET ";
	if (!metadata.output_equal_list_for_update(update_query, row, changed_indexes)) {
		return;
	}
	update_query << " WHERE ";
	if (!metadata.output_equal_list_for_where(update_query, row)) {
		return;
	}

	std::cout << update_query << ";\n";
}

void compute_table_diff(Connection& conn, const TableMetadata& metadata, const std::string& full_table_name,
                        TableData& table_data) {
	std::vector<int> changed_indexes;
	process_rows_from_query(conn, "SELECT * FROM " + full_table_name, [&](const Row& row) {
		PrimaryKey keys = metadata.extract_keys(row);

		auto it = table_data.rows.find(keys);
		if (it == table_data.rows.end()) {
			// if the row is not present in table_data, it should be INSERTed
			print_insert(conn, metadata, row, table_data.full_table_name);
		}
		else {
			// it is present, but it may have changed
			changed_indexes.clear();
			for (int index = 0; index < metadata.field_count; ++index) {
				if (row[index] != it->second[index]) {
					changed_indexes.push_back(index);
				}
			}
			if (!changed_indexes.empty()) {
				print_update(conn, metadata, row, table_data.full_table_name, changed_indexes);
			}
			table_data.rows.erase(it);
		}
	});

	// afterwards, all rows that are left in table_data are the ones that should be DELETEd
	for (const auto& old : table_data.rows) {
		print_delete(conn, metadata, old.second, table_data.full_table_name);
	}
}

void compute_changed_rows_on_db(Connection& conn, const TableMetadata& metadata, const std::string& source_table_name, const std::string& target_table_name) {
	Query select_query = conn.query();
	select_query << "SELECT s.*, t.* FROM " + source_table_name + " s JOIN " + target_table_name + " t USING (";
	if (!metadata.output_key_list_for_using(select_query, {})) {
		return;
	}
	select_query << ") WHERE ";
	if (!metadata.output_diff_list_for_where(select_query, {})) {
		return;
	}

	std::vector<int> changed_indexes;
	process_rows_from_query(conn, select_query, [&](const Row& row) {
		// the rows present in both database, but with different values
		changed_indexes.clear();
		for (int index = 0; index < metadata.field_count; ++index) {
			if (row[index] != row[index + metadata.field_count]) {
				changed_indexes.push_back(index);
			}
		}
		if (!changed_indexes.empty()) {
			print_update(conn, metadata, row, target_table_name, changed_indexes);
		}
	});
}

void compute_new_rows_on_db(Connection& conn, const TableMetadata& metadata, const std::string& source_table_name, const std::string& target_table_name) {
	Query select_query = conn.query();
	select_query << "SELECT s.* FROM " + source_table_name + " s LEFT JOIN " + target_table_name + " j USING (";
	if (!metadata.output_key_list_for_using(select_query, {})) {
		return;
	}
	select_query << ") WHERE ";
	if (!metadata.output_null_key_list_for_where(select_query, {})) {
		return;
	}

	process_rows_from_query(conn, select_query, [&](const Row& row) {
		// rows in source that are not yet in target database
		print_insert(conn, metadata, row, target_table_name);
	});
}

void compute_old_rows_on_db(Connection& conn, const TableMetadata& metadata, const std::string& source_table_name, const std::string& target_table_name) {
	Query select_query = conn.query();
	select_query << "SELECT t.* FROM " + target_table_name + " t LEFT JOIN " + source_table_name + " j USING (";
	if (!metadata.output_key_list_for_using(select_query, {})) {
		return;
	}
	select_query << ") WHERE ";
	if (!metadata.output_null_key_list_for_where(select_query, {})) {
		return;
	}

	process_rows_from_query(conn, select_query, [&](const Row& row) {
		// rows in target that are not in source database anymore
		print_delete(conn, metadata, row, target_table_name);
	});
}

void compute_table_diff_on_db(Connection& conn, const TableMetadata& metadata, const std::string& source_table_name, const std::string& target_table_name) {
	compute_changed_rows_on_db(conn, metadata, source_table_name, target_table_name);
	compute_new_rows_on_db(conn, metadata, source_table_name, target_table_name);
	compute_old_rows_on_db(conn, metadata, source_table_name, target_table_name);
}

int main(int argc, char** argv) {
	if (argc < 4 || argc > 5) {
		std::cerr << "USAGE: dbdpp [ source.cfg ] target.cfg source_table_name target_table_name\n"
			<< "\t(source.cfg and target.cfg should be MySQL-style configuration files)" << std::endl;
		return 1;
	}

	try {
		Config source = ConfigParser(argv[1]).parse_config();
		Config target = ConfigParser(argv[argc-3]).parse_config();
		const char* source_table_name = argv[argc-2];
		const char* target_table_name = argv[argc-1];

		std::shared_ptr<Connection> source_conn, target_conn;
		target_conn = std::make_shared<Connection>(target.database.c_str(), target.host.c_str(), target.user.c_str(), target.password.c_str());
		if (argc == 5) {
			source_conn = std::make_shared<Connection>(source.database.c_str(), source.host.c_str(), source.user.c_str(), source.password.c_str());
		} else {
			source_conn = target_conn;
		}

		TableMetadata metadata = extract_table_metadata(*target_conn, target_table_name);
		if (extract_table_metadata(*source_conn, source_table_name) != metadata) {
			throw std::runtime_error("table definitions differ");
		}

		if (argc == 5) {
			TableData data_in_target = fetch_table_data(*target_conn, metadata, target_table_name);
			compute_table_diff(*source_conn, metadata, source_table_name, data_in_target);

		} else {
			compute_table_diff_on_db(*target_conn, metadata, source_table_name, target_table_name);

		}
	}
	catch (const std::exception& e) {
		std::cerr << "ERROR! " << e.what() << std::endl;
		return 1;
	}

	return 0;
}
