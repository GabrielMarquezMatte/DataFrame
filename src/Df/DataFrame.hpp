#ifndef DATAFRAME_HPP
#define DATAFRAME_HPP
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
        void load(const data_map<T...> &data);
        void load(const data_vec<T...> &data);
        void load_csv(std::string_view path, const char &delimiter);
        void load_csv(const fs::path &path, const char &delimiter);
    };
    #pragma region "Load Functions"
    template <typename... T>
    void DataFrame<T...>::load(const data_map<T...> &data)
    {
        this->data = data;
        this->rows = data.begin()->second.size();
        this->cols = data.size();
        for (auto it = data.begin(); it != data.end(); it++)
        {
            this->columns.push_back(it->first);
        }
    }
    template <typename... T>
    void DataFrame<T...>::load(const data_vec<T...> &data)
    {
        for (auto it = data.begin(); it != data.end(); it++)
        {
            this->data[it->first] = it->second;
        }
        this->rows = data.begin()->second.size();
        this->cols = data.size();
        for (auto it = data.begin(); it != data.end(); it++)
        {
            this->columns.push_back(it->first);
        }
    }
    template <typename... T>
    void DataFrame<T...>::load_csv(std::string_view path, const char &delimiter)
    {
        fs::ifstream file(path);
        if (!file.is_open())
        {
            throw std::runtime_error("File not found");
        }
        std::string line;
        std::getline(file, line);
        std::stringstream ss(line);
        std::string column;
        while (std::getline(ss, column, delimiter))
        {
            this->data[column] = con::vector<boost::variant<T...>>();
            this->columns.push_back(column);
        }
        this->cols = this->columns.size();
        while (std::getline(file, line))
        {
            std::stringstream ss(line);
            std::string value;
            int i = 0;
            while (std::getline(ss, value, delimiter))
            {
                this->data[this->columns[i]].push_back(boost::lexical_cast<boost::variant<T...>>(value));
                i++;
            }
            this->rows++;
        }
        file.close();
    }
    template <typename... T>
    void DataFrame<T...>::load_csv(const fs::path &path, const char &delimiter)
    {
        fs::ifstream file(path);
        if (!file.is_open())
        {
            throw std::runtime_error("File not found");
        }
        std::string line;
        std::getline(file, line);
        std::stringstream ss(line);
        std::string column;
        while (std::getline(ss, column, delimiter))
        {
            this->data[column] = con::vector<boost::variant<T...>>();
            this->columns.push_back(column);
        }
        this->cols = this->columns.size();
        while (std::getline(file, line))
        {
            std::stringstream ss(line);
            std::string value;
            int i = 0;
            while (std::getline(ss, value, delimiter))
            {
                this->data[this->columns[i]].push_back(boost::lexical_cast<boost::variant<T...>>(value));
                i++;
            }
            this->rows++;
        }
        file.close();
    }
    #pragma endregion
    #pragma region "DataFrame Operations"
    template <typename... T>
    void DataFrame<T...>::print()
    {
        for (auto it = data.begin(); it != data.end(); it++)
        {
            std::cout << it->first << "\t";
        }
        std::cout << "\n";
        for (int i = 0; i < rows; i++)
        {
            for (auto it = data.begin(); it != data.end(); it++)
            {
                std::cout << it->second[i] << "\t";
            }
            std::cout << "\n";
        }
    }
    template <typename... T>
    void DataFrame<T...>::concat(const DataFrame<T...> df)
    {
        DataFrame<T...> newDf(data);
        int new_rows = rows + df.rows;
        for (auto it = df.data.begin(); it != df.data.end(); it++)
        {
            if (columns.find(it->first) == columns.end())
            {
                columns.insert(it->first);
                cols++;
                newDf.data[it->first] = con::vector<boost::variant<T...>>();
            }
            newDf.data[it->first].reserve(new_rows);
        }
        for (auto it = df.data.begin(); it != df.data.end(); it++)
        {
            for (int i = 0; i < df.rows; i++)
            {
                newDf.data[it->first].push_back(it->second[i]);
            }
        }
        newDf.rows += df.rows;
        *this = newDf;
    }
    template <typename... T>
    void DataFrame<T...>::setPrimaryKey(const column_set &primary_key)
    {
        this->primaryKeys = primary_key;
    }
    template <typename... T>
    const column_set DataFrame<T...>::getPrimaryKey() const
    {
        return this->primaryKeys;
    }
    template <typename... T>
    const int DataFrame<T...>::getRows() const
    {
        return this->rows;
    }
    template <typename... T>
    const int DataFrame<T...>::getCols() const
    {
        return this->cols;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::select(column_set &columns)
    {
        DataFrame<T...> newDf;
        for (auto it = columns.begin(); it != columns.end(); it++)
        {
            if (this->columns.find(*it) != this->columns.end())
            {
                newDf.data[*it] = this->data.at(*it);
            }
        }
        newDf.rows = this->rows;
        newDf.cols = columns.size();
        newDf.columns = columns;
        return newDf;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::select(std::string_view column)
    {
        const DataFrame<T...> newDf;
        newDf.data[column] = this->data.at(column);
        newDf.rows = this->rows;
        newDf.cols = 1;
        newDf.columns = column;
        return newDf;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::find(std::string_view column, const T &...value)
    {
        const DataFrame<T...> newDf;
        int i = 0;
        for (auto it = this->data.at(column).begin(); it != this->data.at(column).end(); it++)
        {
            if (std::find(std::begin({value...}), std::end({value...}), *it) != std::end({value...}))
            {
                for (auto it2 = this->data.begin(); it2 != this->data.end(); it2++)
                {
                    if (newDf.data.find(it2->first) == newDf.data.end())
                    {
                        newDf.data[it2->first] = con::vector<boost::variant<T...>>();
                    }
                    newDf.data[it2->first].push_back(it2->second[i]);
                }
                newDf.rows++;
            }
        }
        newDf.cols = this->cols;
        newDf.columns = this->columns;
        return newDf;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::join(const DataFrame<T...> &df, const column_set &columns, JoinTypes joinType)
    {
        DataFrame<T...> newDf;
        switch (joinType)
        {
        case JoinTypes::INNER:
            for (auto it = this->data.begin(); it != this->data.end(); it++)
            {
                if (df.data.find(it->first) != df.data.end())
                {
                    newDf.data[it->first] = con::vector<boost::variant<T...>>();
                }
            }
            for (auto it = df.data.begin(); it != df.data.end(); it++)
            {
                if (this->data.find(it->first) != this->data.end())
                {
                    newDf.data[it->first] = con::vector<boost::variant<T...>>();
                }
            }
            for (int i = 0; i < this->rows; i++)
            {
                for (int j = 0; j < df.rows; j++)
                {
                    bool match = true;
                    for (auto it = columns.begin(); it != columns.end(); it++)
                    {
                        if (this->data.at(*it)[i] != df.data.at(*it)[j])
                        {
                            match = false;
                            break;
                        }
                    }
                    if (match)
                    {
                        for (auto it = newDf.data.begin(); it != newDf.data.end(); it++)
                        {
                            if (this->data.find(it->first) != this->data.end())
                            {
                                it->second.push_back(this->data.at(it->first)[i]);
                            }
                            else
                            {
                                it->second.push_back(df.data.at(it->first)[j]);
                            }
                        }
                        newDf.rows++;
                    }
                }
            }
            break;
        case JoinTypes::LEFT:
            for (auto it = this->data.begin(); it != this->data.end(); it++)
            {
                newDf.data[it->first] = con::vector<boost::variant<T...>>();
            }
            for (auto it = df.data.begin(); it != df.data.end(); it++)
            {
                if (this->data.find(it->first) != this->data.end())
                {
                    newDf.data[it->first] = con::vector<boost::variant<T...>>();
                }
            }
            for (int i = 0; i < this->rows; i++)
            {
                bool match = false;
                for (int j = 0; j < df.rows; j++)
                {
                    bool match2 = true;
                    for (auto it = columns.begin(); it != columns.end(); it++)
                    {
                        if (this->data.at(*it)[i] != df.data.at(*it)[j])
                        {
                            match2 = false;
                            break;
                        }
                    }
                    if (match2)
                    {
                        match = true;
                        for (auto it = newDf.data.begin(); it != newDf.data.end(); it++)
                        {
                            if (this->data.find(it->first) != this->data.end())
                            {
                                it->second.push_back(this->data.at(it->first)[i]);
                            }
                            else
                            {
                                it->second.push_back(df.data.at(it->first)[j]);
                            }
                        }
                        newDf.rows++;
                    }
                }
                if (!match)
                {
                    for (auto it = newDf.data.begin(); it != newDf.data.end(); it++)
                    {
                        if (this->data.find(it->first) != this->data.end())
                        {
                            it->second.push_back(this->data.at(it->first)[i]);
                        }
                        else
                        {
                            it->second.push_back(boost::variant<T...>());
                        }
                    }
                    newDf.rows++;
                }
            }
            break;
        }
        return newDf;
    }
    #pragma endregion
    #pragma region "Constructors"
    template <typename... T>
    DataFrame<T...>::DataFrame() {}
    template <typename... T>
    DataFrame<T...>::~DataFrame()
    {
        data.clear();
        columns.clear();
        primaryKeys.clear();
    }
    template <typename... T>
    DataFrame<T...>::DataFrame(const data_map<T...> &data, const column_set &primaryKeys)
    {
        this->data = data;
        for (auto it = data.begin(); it != data.end(); it++)
        {
            columns.insert(it->first);
            cols++;
        }
        rows = data.begin()->second.size();
        this->primaryKeys = primaryKeys;
    }
    template <typename... T>
    DataFrame<T...>::DataFrame(const data_map<T...> &data)
    {
        this->data = data;
        for (auto it = data.begin(); it != data.end(); it++)
        {
            columns.insert(it->first);
            cols++;
        }
        rows = data.begin()->second.size();
    }
    template <typename... T>
    DataFrame<T...>::DataFrame(const data_vec<T...> &data)
    {
        for (auto it = data.begin(); it != data.end(); it++)
        {
            for (auto it2 = it->begin(); it2 != it->end(); it2++)
            {
                if (columns.find(it2->first) == columns.end())
                {
                    columns.insert(it2->first);
                    cols++;
                }
            }
        }
        rows = data.size();
        this->data = data_map<T...>();
        for (auto it = columns.begin(); it != columns.end(); it++)
        {
            this->data[*it] = con::vector<boost::variant<T...>>(rows);
        }
        int i = 0;
        for (auto it = data.begin(); it != data.end(); it++)
        {
            for (std::string col : columns)
            {
                if (it->find(col) == it->end())
                {
                    this->data[col][i] = boost::none;
                }
                else
                {
                    this->data[col][i] = it->at(col);
                }
            }
            i++;
        }
    }
    #pragma endregion
}
#endif