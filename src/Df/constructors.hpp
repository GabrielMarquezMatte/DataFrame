#ifndef CONSTRUCTORS_HPP
#define CONSTRUCTORS_HPP
#include "class_header.hpp"
namespace df{
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
}
#endif