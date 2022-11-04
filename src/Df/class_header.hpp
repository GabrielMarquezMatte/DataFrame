#ifndef DATAFRAME_CLASS_HEADER_HPP
#define DATAFRAME_CLASS_HEADER_HPP
#include <iostream>
#include <string>
#ifdef USE_BOOST
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/container/vector.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/variant.hpp>
#include <boost/sort/sort.hpp>
#include <boost/lexical_cast.hpp>
namespace fs = boost::filesystem;
namespace con = boost::container;
#else
#include <filesystem>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>
#endif
namespace df
{
    template <typename... T>
    #ifdef USE_BOOST
    using data_map = boost::unordered_map<std::string, boost::container::vector<boost::variant<T...>>>;
    template <typename... T>
    using data_vec = con::vector<boost::unordered_map<std::string, boost::variant<T...>>>;
    template <typename... T>
    using series = con::vector<boost::variant<T...>>;
    using column_set = boost::unordered_set<std::string>;
    template <typename... T>
    using na_value = boost::variant<T...>;
    template <typename... T>
    using vector = con::vector<T...>;
    template <typename... T>
    using value_t = boost::variant<T...>;
    #else
    using data_map = std::unordered_map<std::string, std::vector<std::variant<T...>>>;
    template <typename... T>
    using data_vec = std::vector<std::unordered_map<std::string, std::variant<T...>>>;
    template <typename... T>
    using series = std::vector<std::variant<T...>>;
    using column_set = std::unordered_set<std::string>;
    template <typename... T>
    using na_value = std::variant<T...>;
    template <typename... T>
    using vector = std::vector<T...>;
    template <typename... T>
    using value_t = std::variant<T...>;
    #endif
    enum JoinTypes
    {
        LEFT,
        INNER
    };
    template <typename... T>
    class DataFrame
    {
    private:
        data_map<T...> data = {};
        column_set columns = {};
        int cols = 0;
        int rows = 0;
        column_set primaryKeys = {};

    public:
        ~DataFrame();
        DataFrame();
        DataFrame(const data_map<T...> &data, const column_set &primaryKeys);
        DataFrame(const data_map<T...> &data);
        DataFrame(const data_vec<T...> &data);
        void print();
        void concat(const DataFrame<T...> df);
        void setPrimaryKey(const column_set &primary_key);
        const column_set getPrimaryKey() const;
        const int getRows() const;
        const int getCols() const;
        DataFrame<T...> select(column_set &columns);
        DataFrame<T...> select(const std::string& column);
        DataFrame<T...> find(const std::string& column, const value_t<T...> &value);
        DataFrame<T...> join(const DataFrame<T...> &df, const column_set &columns, JoinTypes type);
        value_t<T...> get(const int& row,int col);
        DataFrame<T...> sort(const std::string& column);
        DataFrame<T...> sort(const column_set& columns);
        DataFrame<T...> distinct(const std::string& column);
        DataFrame<T...> distinct(const column_set& columns);
        DataFrame<T...> distinct();
        void load(const data_map<T...> &data);
        void load(const data_vec<T...> &data);
        void load_csv(const std::string& path, const char &delimiter = ";");
        #ifdef USE_BOOST
        void load_csv(const fs::path &path, const char &delimiter = ";");
        void write_csv(const fs::path &path, const char &delimiter = ";");
        #endif
        void write_csv(const std::string& path, const char &delimiter = ";");
        bool operator==(const DataFrame<T...> &df);
        bool operator!=(const DataFrame<T...> &df);
    };
}
#endif