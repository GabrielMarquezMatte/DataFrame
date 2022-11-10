#ifndef DATAFRAME_LOAD_DATA_HPP
#define DATAFRAME_LOAD_DATA_HPP
#include "class_header.hpp"
#include <stdio.h>
namespace df
{
    template <typename... T>
    void DataFrame<T...>::load(const data_map<T...> &data)
    {
        this->data = data;
        this->rows = data.begin()->second.size();
        this->cols = data.size();
        for (auto it = data.begin(); it != data.end(); it++)
        {
            this->columns.insert(it->first);
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
    void DataFrame<T...>::load_csv(const std::string &path, const char &delimiter)
    {
#ifdef USE_BOOST
        fs::ifstream file(path);
#else
        std::ifstream file(path);
#endif
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
            this->data[column] = series<T...>();
            this->columns.insert(column);
        }
        this->cols = this->columns.size();
        while (std::getline(file, line))
        {
            std::stringstream ss(line);
            std::string value;
            int i = 0;
            while (std::getline(ss, value, delimiter))
            {
                auto it = this->columns.begin();
#ifdef USE_BOOST
                boost::advance(it, i);
#else
                std::advance(it, i);
#endif
                std::string column = *it;
                this->data[column].push_back(value);
                i++;
            }
            this->rows++;
        }
        file.close();
    }
#ifdef USE_BOOST
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
            this->data[column] = series<T...>();
            this->columns.insert(column);
        }
        this->cols = this->columns.size();
        while (std::getline(file, line))
        {
            std::stringstream ss(line);
            std::string value;
            int i = 0;
            while (std::getline(ss, value, delimiter))
            {
                auto it = this->columns.begin();
#ifdef USE_BOOST
                boost::advance(it, i);
#else
                std::advance(it, i);
#endif
                std::string &column = *it;
                this->data[column].push_back(boost::lexical_cast<boost::variant<T...>>(value));
                i++;
            }
            this->rows++;
        }
        file.close();
    }
    template <typename... T>
    void DataFrame<T...>::write_csv(const fs::path &path, const char &delimiter)
    {
        try
        {
            fs::ofstream file(path);
            for (auto &column : this->columns)
            {
                file << column << delimiter;
            }
            file << "\n";
            for (int i = 0; i < this->rows; i++)
            {
                for (auto &column : this->columns)
                {
                    file << boost::lexical_cast<std::string>(this->data[column].at(i)) << delimiter;
                }
                file << "\n";
            }
            file.close();
        }
        catch (const std::exception &e)
        {
            std::cout << e.what() << std::endl;
        }
    }
#endif
    template <typename... T>
    void DataFrame<T...>::write_csv(const std::string &path, const char &delimiter)
    {
        try
        {
            std::string ss;
            for (auto &column : this->columns)
            {
                ss += column + delimiter;
            }
            ss += "\n";
            for (int i = 0; i < this->rows; i++)
            {
                for (auto &column : this->columns)
                {
#ifdef USE_BOOST
                    ss += boost::lexical_cast<std::string>(this->data[column].at(i)) += delimiter;
#else
                    ss += std::to_string(this->data[column].at(i)) += delimiter;
#endif
                }
                ss += "\n";
            }
            FILE *file = fopen(path.c_str(), "w");
            fwrite(ss.c_str(), sizeof(char), ss.size(), file);
            fclose(file);
        }
        catch (const std::exception &e)
        {
            std::cout << e.what() << std::endl;
        }
    }
    template <typename... T>
    void DataFrame<T...>::write_xlsx(const std::string &path)
    {
        xlnt::workbook wb;
        xlnt::worksheet ws = wb.active_sheet();
        int row = 1;
        int col = 1;
        for (auto &column : this->columns)
        {
            ws.cell(row, col).value(column);
            col++;
        }
        row++;
        col = 1;
        for (int i = 0; i < this->rows; i++)
        {
            for (auto &column : this->columns)
            {
                auto value = this->data[column].at(i);
                if (std::is_arithmetic<decltype(value)>::value)
                {
#ifdef USE_BOOST
                    ws.cell(row, col).value(boost::lexical_cast<double>(value));
#else
                    ws.cell(row, col).value(std::stod(value));
#endif
                }
                else
                {
#ifdef USE_BOOST
                    ws.cell(row, col).value(boost::lexical_cast<std::string>(value));
#else
                    ws.cell(row, col).value(value);
#endif
                }
                col++;
            }
            row++;
            col = 1;
        }
        wb.save(path);
    }
}
#endif