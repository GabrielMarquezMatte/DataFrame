#define USE_BOOST
#include "Df/DataFrame.hpp"
#include <chrono>
bool SelectTest()
{
    try
    {
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        bool teste1 = (df.getCols() == 2);
        bool teste2 = (df.getRows() == 10);
        df::DataFrame<int64_t, std::string> df2 = df.select(df::column_set{"id"});
        bool teste3 = (df2.getCols() == 1);
        bool teste4 = (df2.getRows() == 10);
        return teste1 && teste2 && teste3 && teste4;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool ConcatTest()
{
    try
    {
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        bool teste1 = (df.getRows() == 10);
        df::data_map<int64_t, std::string> data2 = {
            {"id", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {110, 120, 130, 140, 150, 160, 170, 180, 190, 200}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        bool teste2 = (df2.getRows() == 10);
        df.concat(df2);
        bool teste3 = (df.getRows() == 20);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool GetRowsAndColsTest()
{
    try
    {
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        bool teste1 = (df.getRows() == 10);
        bool teste2 = (df.getCols() == 2);
        return teste1 && teste2;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool LeftJoinTest()
{
    try
    {
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        df::data_map<int64_t, std::string> data2 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        df::DataFrame<int64_t, std::string> df3 = df.join(df2, df::column_set{"id"}, df::JoinTypes::LEFT);
        df::data_map<int64_t,std::string> data3 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df4(data3);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 3);
        bool teste3 = (df3 == df4);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool JoinMultiple()
{
    try
    {
        df::data_map<int64_t, std::string> data = {
            {"id1", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"id2", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        df::data_map<int64_t, std::string> data2 = {
            {"id1", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"id2", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        df::DataFrame<int64_t, std::string> df3 = df.join(df2, df::column_set{"id1","id2"}, df::JoinTypes::INNER);
        df::data_map<int64_t,std::string> data3 = {
            {"id1", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"id2", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df4(data3);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 4);
        bool teste3 = (df3 == df4);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool JoinNA()
{
    try
    {
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        df::data_map<int64_t, std::string> data2 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i"}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        df::DataFrame<int64_t, std::string> df3 = df.join(df2, df::column_set{"id"}, df::JoinTypes::LEFT);
        df::data_map<int64_t,std::string> data3 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df4(data3);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 3);
        bool teste3 = (df3 != df4);
        df3.print();
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
int main()
{
    std::chrono::steady_clock::time_point start = std::chrono::high_resolution_clock::now();
    std::stringstream ss;
    if (SelectTest())
    {
        ss << "Test 1 passed\n";
    }
    else
    {
        ss << "Test 1 failed\n";
    }
    if (ConcatTest())
    {
        ss << "Test 2 passed\n";
    }
    else
    {
        ss << "Test 2 failed\n";
    }
    if (GetRowsAndColsTest())
    {
        ss << "Test 3 passed\n";
    }
    else
    {
        ss << "Test 3 failed\n";
    }
    if (LeftJoinTest())
    {
        ss << "Test 4 passed\n";
    }
    else
    {
        ss << "Test 4 failed\n";
    }
    if (JoinMultiple())
    {
        ss << "Test 5 passed\n";
    }
    else
    {
        ss << "Test 5 failed\n";
    }
    if (JoinNA())
    {
        ss << "Test 6 passed\n";
    }
    else
    {
        ss << "Test 6 failed\n";
    }
    std::cout << ss.str();
    std::chrono::steady_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << " microseconds" << std::endl;
    return 0;
}