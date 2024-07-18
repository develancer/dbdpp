#include <fstream>
#include <iostream>
#include <map>
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

void compute_table_diff(Connection& conn, const TableMetadata& metadata, const std::string& full_table_name,
                        TableData& table_data) {
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

int main(int argc, char** argv) {
	if (argc < 4 || argc > 5) {
		std::cerr << "USAGE: dbdpp source.cfg target.cfg source_table_name [ target_table_name ]\n"
			<< "\t(source.cfg and target.cfg should be MySQL-style configuration files)" << std::endl;
		return 1;
	}

	try {
		Config source = ConfigParser(argv[1]).parse_config();
		Config target = ConfigParser(argv[2]).parse_config();
		const char* source_table_name = argv[3];
		const char* target_table_name = argv[argc-1];

		Connection target_conn(target.database.c_str(), target.host.c_str(), target.user.c_str(), target.password.c_str());
		Connection source_conn(source.database.c_str(), source.host.c_str(), source.user.c_str(), source.password.c_str());

		TableMetadata metadata = extract_table_metadata(target_conn, target_table_name);
		if (extract_table_metadata(source_conn, source_table_name) != metadata) {
			throw std::runtime_error("table definitions differ");
		}

		TableData data_in_target = fetch_table_data(target_conn, metadata, target_table_name);
		compute_table_diff(source_conn, metadata, source_table_name, data_in_target);
	}
	catch (const std::exception& e) {
		std::cerr << "ERROR! " << e.what() << std::endl;
		return 1;
	}

	return 0;
}
