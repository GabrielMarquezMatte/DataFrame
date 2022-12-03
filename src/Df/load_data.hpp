#ifndef DATAFRAME_LOAD_DATA_HPP
#define DATAFRAME_LOAD_DATA_HPP
#include "class_header.hpp"
#include <stdio.h>
#ifdef USE_BOOST
#include <boost/regex.hpp>
#else
#include <regex>
#endif
namespace df
{
    bool isNumber(const std::string& value){
        if(value.empty()) return false;
        #ifdef USE_BOOST
        boost::regex pattern("^[-+]?[0-9]*\\.?[0-9]+$");
        return boost::regex_match(value, pattern);
        #else
        std::regex pattern("^[-+]?[0-9]*\\.?[0-9]+$");
        return std::regex_match(value, pattern);
        #endif
    }
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
#ifdef USE_BOOST
    template <typename ...T>
    xlnt::workbook DataFrame<T...>::create_workbook(){
        xlnt::workbook wb;
        xlnt::worksheet ws = wb.active_sheet();
        int row = 1;
        int col = 1;
        for (const std::string &column : this->columns)
        {
            ws.cell(col, row).value(column);
            col++;
        }
        row++;
        col = 1;
        for (int i = 0; i < this->rows; i++)
        {
            for (const std::string &column : this->columns)
            {
                value_t<T...> value = this->data[column].at(i);
                #ifdef USE_BOOST
                const std::string str = boost::lexical_cast<std::string>(value);
                #else
                const std::string str = std::to_string(value);
                #endif
                if(isNumber(str))
                {
                    #ifdef USE_BOOST
                    ws.cell(col, row).value(boost::lexical_cast<double>(str));
                    #else
                    ws.cell(col, row).value(std::stod(str));
                    #endif
                }
                else
                {
                    ws.cell(col, row).value(str);
                }
                col++;
            }
            row++;
            col = 1;
        }
        return wb;
    }
    template <typename... T>
    void DataFrame<T...>::write_xlsx(const std::string &path)
    {
        xlnt::workbook wb = this->create_workbook();
        wb.save(path);
    }
    template <typename... T>
    void DataFrame<T...>::write_xlsx(const char *path){
        this->write_xlsx(std::string(path));
    }
    template <typename... T>
    void DataFrame<T...>::write_xlsx(const std::string& path,const std::string &password){
        xlnt::workbook wb = this->create_workbook();
        wb->save(path,password);
    }
    template <typename... T>
    void DataFrame<T...>::load_xlsx(const std::string& path){
        xlnt::workbook wb;
        wb.load(path);
        xlnt::worksheet ws = wb.active_sheet();
        xlnt::range_reference range = ws.calculate_dimension();
        xlnt::cell_reference top_left = range.top_left();
        xlnt::cell_reference bottom_right = range.bottom_right();
        int row = top_left.row();
        int col = top_left.column();
        for (int i = col; i <= bottom_right.column(); i++)
        {
            std::string column = ws.cell(i, row).to_string();
            this->data[column] = series<T...>();
            this->columns.insert(column);
        }
        this->cols = this->columns.size();
        row++;
        for (int i = row; i <= bottom_right.row(); i++)
        {
            for (int j = col; j <= bottom_right.column(); j++)
            {
                auto it = this->columns.begin();
                boost::advance(it, j - col);
                std::string &column = *it;
                this->data[column].push_back(boost::lexical_cast<boost::variant<T...>>(ws.cell(j, i).to_string()));
            }
            this->rows++;
        }
    }
#endif
}
#endif