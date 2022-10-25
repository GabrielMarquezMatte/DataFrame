#ifndef DATAFRAME_OPERATIONS_HPP
#define DATAFRAME_OPERATIONS_HPP
#include "class_header.hpp"
namespace df
{
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
    template<typename ...T>
    boost::variant<T...> DataFrame<T...>::get(const int& row,const int& col)
    {
        return this->data.at(this->columns[col])[row];
    }
}
#endif