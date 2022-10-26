#ifndef DATAFRAME_CLASS_HEADER_HPP
#define DATAFRAME_CLASS_HEADER_HPP
#include <iostream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/container/vector.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/variant.hpp>
namespace fs = boost::filesystem;
namespace con = boost::container;
namespace df
{
    template <typename... T>
    using data_map = boost::unordered_map<std::string, boost::container::vector<boost::variant<T...>>>;
    template <typename... T>
    using data_vec = con::vector<boost::unordered_map<std::string, boost::variant<T...>>>;
    using column_set = boost::unordered_set<std::string>;
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
        DataFrame<T...> select(std::string_view column);
        DataFrame<T...> find(std::string_view column, const T &...value);
        DataFrame<T...> join(const DataFrame<T...> &df, const column_set &columns, JoinTypes type);
        boost::variant<T...> get(const int& row,int col);
        void load(const data_map<T...> &data);
        void load(const data_vec<T...> &data);
        void load_csv(std::string_view path, const char &delimiter);
        void load_csv(const fs::path &path, const char &delimiter);
    };
}
#endif