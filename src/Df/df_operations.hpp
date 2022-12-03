#ifndef DATAFRAME_OPERATIONS_HPP
#define DATAFRAME_OPERATIONS_HPP
#include "class_header.hpp"
#include <sstream>
namespace df
{
    template <typename... T>
    void DataFrame<T...>::print()
    {
        std::stringstream ss;
        for (auto it = data.begin(); it != data.end(); it++)
        {
            ss << it->first << "\t";
        }
        ss << "\n";
        for (int i = 0; i < rows; i++)
        {
            for (auto it = data.begin(); it != data.end(); it++)
            {
                ss << it->second[i] << "\t";
            }
            ss << "\n";
        }
        std::cout << ss.str();
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
                newDf.data[it->first] = series<T...>();
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
    DataFrame<T...> DataFrame<T...>::select(const std::string &column)
    {
        const DataFrame<T...> newDf;
        newDf.data[column] = this->data.at(column);
        newDf.rows = this->rows;
        newDf.cols = 1;
        newDf.columns = column;
        return newDf;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::find(const std::string &column, const value_t<T...> &value)
    {
        DataFrame<T...> newDf;
        int i = 0;
        for (int j = 0; j < this->rows; j++)
        {
            if (this->data.at(column)[j] == value)
            {
                for (auto it = this->data.begin(); it != this->data.end(); it++)
                {
                    newDf.data[it->first].push_back(it->second[j]);
                }
                i++;
            }
        }
        newDf.rows = i;
        newDf.cols = this->cols;
        newDf.columns = this->columns;
        return newDf;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::join(const DataFrame<T...> &df, const column_set &columns, JoinTypes joinType)
    {
        DataFrame<T...> newDf;
        newDf.cols = this->cols + df.cols - columns.size();
        newDf.columns = this->columns;
        newDf.columns.insert(df.columns.begin(), df.columns.end());
        switch (joinType)
        {
        case JoinTypes::INNER:
            newDf.data = this->data;
            for (auto it = df.data.begin(); it != df.data.end(); it++)
            {
                if (this->data.find(it->first) == this->data.end())
                {
                    newDf.data[it->first] = series<T...>();
                    newDf.data[it->first].reserve(this->rows);
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
                newDf.data[it->first] = series<T...>();
                newDf.data[it->first].reserve(this->rows + df.rows);
            }
            for (auto it = df.data.begin(); it != df.data.end(); it++)
            {
                if (this->data.find(it->first) == this->data.end())
                {
                    newDf.data[it->first] = series<T...>();
                    newDf.data[it->first].reserve(this->rows + df.rows);
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
                            it->second.push_back(na_value<T...>());
                        }
                    }
                    newDf.rows++;
                }
            }
            break;
        }
        return newDf;
    }
    template <typename... T>
    value_t<T...> DataFrame<T...>::get(const int &row, int col)
    {
        for (auto it = this->data.begin(); it != this->data.end(); it++)
        {
            if (col == 0)
            {
                return it->second[row];
            }
            col--;
        }
        throw std::out_of_range("Column out of range");
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::sort(const std::string &column)
    {
        DataFrame<T...> newDf;
        newDf.data = this->data;
        newDf.rows = this->rows;
        newDf.cols = this->cols;
        newDf.columns = this->columns;
        // Sort all columns by the given column
        for (auto it = newDf.data.begin(); it != newDf.data.end(); it++)
        {
            vector<std::pair<value_t<T...>, int>> pairs;
            for (int i = 0; i < newDf.rows; i++)
            {
                pairs.push_back(std::make_pair(newDf.data.at(column)[i], i));
            }
            std::sort(pairs.begin(), pairs.end());
            for (auto it2 = newDf.data.begin(); it2 != newDf.data.end(); it2++)
            {
                series<T...> newCol;
                newCol.reserve(newDf.rows);
                for (int i = 0; i < newDf.rows; i++)
                {
                    newCol.push_back(newDf.data.at(it2->first)[pairs[i].second]);
                }
                newDf.data[it2->first] = newCol;
            }
        }
        return newDf;
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::sort(const column_set &columns)
    {
        DataFrame<T...> newDf;
        newDf.data = this->data;
        newDf.rows = this->rows;
        newDf.cols = this->cols;
        newDf.columns = this->columns;
        // Sort all columns by the given columns
        for (auto it = newDf.data.begin(); it != newDf.data.end(); it++)
        {
            vector<std::pair<value_t<T...>, int>> pairs;
            for (int i = 0; i < newDf.rows; i++)
            {
                value_t<T...> val = 0;
                for (auto it2 = columns.begin(); it2 != columns.end(); it2++)
                {
                    val += newDf.data.at(*it2)[i];
                }
                pairs.push_back(std::make_pair(val, i));
            }
            std::sort(pairs.begin(), pairs.end());
            for (auto it2 = newDf.data.begin(); it2 != newDf.data.end(); it2++)
            {
                series<T...> newCol;
                newCol.reserve(newDf.rows);
                for (int i = 0; i < newDf.rows; i++)
                {
                    newCol.push_back(newDf.data.at(it2->first)[pairs[i].second]);
                }
                newDf.data[it2->first] = newCol;
            }
        }
        return newDf;
    }
    template <typename... T>
    bool DataFrame<T...>::operator==(const DataFrame<T...> &df)
    {
        if (this->rows != df.rows || this->cols != df.cols)
        {
            return false;
        }
        for (auto it = this->data.begin(); it != this->data.end(); it++)
        {
            if (df.data.find(it->first) == df.data.end())
            {
                return false;
            }
            for (int i = 0; i < this->rows; i++)
            {
                if (it->second[i] != df.data.at(it->first)[i])
                {
                    return false;
                }
            }
        }
        return true;
    }
    template <typename... T>
    bool DataFrame<T...>::operator!=(const DataFrame<T...> &df)
    {
        return !(*this == df);
    }
    template <typename... T>
    series<T...> DataFrame<T...>::operator[](const std::string &column)
    {
        return this->data.at(column);
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::operator[](const column_set &columns)
    {
        DataFrame<T...> newDf;
        for (auto it = columns.begin(); it != columns.end(); it++)
        {
            newDf.data[*it] = this->data.at(*it);
        }
        newDf.rows = this->rows;
        newDf.cols = columns.size();
        newDf.columns = columns;
        return newDf;
    }
    template<typename ...T>
    series<T...> DataFrame<T...>::operator[](const int &column)
    {
        int i = 0;
        for (auto it = this->data.begin(); it != this->data.end(); it++)
        {
            if (i == column)
            {
                return it->second;
            }
            i++;
        }
        throw std::out_of_range("Column out of range");
    }
    template <typename... T>
    DataFrame<T...> DataFrame<T...>::operator=(const DataFrame<T...> &df)
    {
        this->data = df.data;
        this->rows = df.rows;
        this->cols = df.cols;
        this->columns = df.columns;
        return *this;
    }
}
#endif