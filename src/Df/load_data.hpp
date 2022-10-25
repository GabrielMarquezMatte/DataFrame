#ifndef DATAFRAME_LOAD_DATA_HPP
#define DATAFRAME_LOAD_DATA_HPP
#include "class_header.hpp"
namespace df{
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
}
#endif