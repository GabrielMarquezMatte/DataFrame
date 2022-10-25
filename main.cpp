#include "Df/DataFrame.hpp"
bool SelectTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        bool teste1 = (df.getCols() == 2);
        bool teste2 = (df.getRows() == 10);
        df::DataFrame<int, std::string> df2 = df.select(df::column_set{"id"});
        bool teste3 = (df2.getCols() == 1);
        bool teste4 = (df2.getRows() == 10);
        return teste1 && teste2 && teste3 && teste4;
    }
    catch (std::exception &e)
    {
        return false;
    }
}
bool ConcatTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        bool teste1 = (df.getRows() == 10);
        df::data_map<int, std::string> data2 = {
            {"id", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {110, 120, 130, 140, 150, 160, 170, 180, 190, 200}}};
        df::DataFrame<int, std::string> df2(data2);
        bool teste2 = (df2.getRows() == 10);
        df.concat(df2);
        bool teste3 = (df.getRows() == 20);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        return false;
    }
}
bool GetRowsAndColsTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        bool teste1 = (df.getRows() == 10);
        bool teste2 = (df.getCols() == 2);
        return teste1 && teste2;
    }
    catch (std::exception &e)
    {
        return false;
    }
}
bool LeftJoinTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        df::data_map<int, std::string> data2 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int, std::string> df2(data2);
        df::DataFrame<int, std::string> df3 = df.join(df2, df::column_set{"id"}, df::JoinTypes::LEFT);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 3);
        bool teste3 = boost::get<std::string>(df3.get(0,2)) == "a";
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        return false;
    }
}
int main()
{
    if (SelectTest())
    {
        std::cout << "Test 1 passed\n";
    }
    else
    {
        std::cout << "Test 1 failed\n";
    }
    if (ConcatTest())
    {
        std::cout << "Test 2 passed\n";
    }
    else
    {
        std::cout << "Test 2 failed\n";
    }
    if (GetRowsAndColsTest())
    {
        std::cout << "Test 3 passed\n";
    }
    else
    {
        std::cout << "Test 3 failed\n";
    }
    if(LeftJoinTest())
    {
        std::cout << "Test 4 passed\n";
    }
    else
    {
        std::cout << "Test 4 failed\n";
    }
    return 0;
}